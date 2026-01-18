import base64
import hmac
import os
import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Dict, List, Optional

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBasic, HTTPBearer
from fastapi.security.http import HTTPBasicCredentials
from starlette.requests import Request

DEFAULT_USERS = "admin:admin123:admin;operator:op123:operator;viewer:view123:viewer"
TOKEN_TTL_SECONDS = 60 * 60 * 12  # 12h tokens for prototype


@dataclass
class User:
    username: str
    role: str


def _load_users() -> Dict[str, str]:
    users_raw = os.environ.get("APP_USERS", DEFAULT_USERS)
    users: Dict[str, str] = {}
    for part in users_raw.split(";"):
        if not part.strip():
            continue
        try:
            username, password, role = part.split(":", 2)
        except ValueError:
            continue
        users[username.strip()] = f"{password.strip()}|{role.strip()}"
    return users


def _secret_key() -> bytes:
    key = os.environ.get("APP_SECRET_KEY")
    if not key:
        key = "dev-secret-key"
    return key.encode("utf-8")


def is_demo_mode() -> bool:
    return os.environ.get("APP_DEMO", "1") == "1"


def create_token(username: str, role: str) -> str:
    exp = int(time.time()) + TOKEN_TTL_SECONDS
    payload = f"{username}:{role}:{exp}".encode("utf-8")
    sig = hmac.new(_secret_key(), payload, sha256).digest()
    token = base64.urlsafe_b64encode(payload + b"." + sig).decode("utf-8")
    return token


def demo_token() -> str:
    if not is_demo_mode():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Demo mode disabled")
    users = _load_users()
    for username, raw in users.items():
        password, role = raw.split("|", 1)
        if role == "viewer":
            return create_token(username, role)
    username, raw = next(iter(users.items()))
    _, role = raw.split("|", 1)
    return create_token(username, role)


def _verify_token(token: str) -> Optional[User]:
    try:
        raw = base64.urlsafe_b64decode(token.encode("utf-8"))
    except Exception:
        return None
    if b"." not in raw:
        return None
    payload, sig = raw.rsplit(b".", 1)
    expected = hmac.new(_secret_key(), payload, sha256).digest()
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        username, role, exp_str = payload.decode("utf-8").split(":")
        if int(exp_str) < int(time.time()):
            return None
    except Exception:
        return None
    return User(username=username, role=role)


basic_scheme = HTTPBasic(auto_error=False)
bearer_scheme = HTTPBearer(auto_error=False)


def login(credentials: HTTPBasicCredentials = Security(basic_scheme)) -> str:
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing credentials")
    users = _load_users()
    stored = users.get(credentials.username)
    if not stored:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    password, role = stored.split("|", 1)
    if not hmac.compare_digest(password, credentials.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    return create_token(credentials.username, role)


def get_current_user(
    bearer: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
    request: Request = None,
) -> User:
    token = bearer.credentials if bearer else None
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    user = _verify_token(token)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")
    request.state.user = user
    return user


def require_role(allowed: List[str]):
    def checker(user: User = Depends(get_current_user)) -> User:
        if user.role not in allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
        return user

    return checker
