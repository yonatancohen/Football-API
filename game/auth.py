from datetime import datetime, timedelta
from typing import Optional

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer


class JWTAuth:
    def __init__(
            self,
            secret_key: str,
            algorithm: str,
            expires_minutes: int,
            username: str,
            password: str,
    ):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.expires_delta = timedelta(minutes=expires_minutes)
        self._username = username
        self._password = password

        self.oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

    def authenticate_user(self, username: str, password: str) -> bool:
        return username == self._username and password == self._password

    def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None) -> str:
        to_encode = data.copy()
        expire = datetime.utcnow() + (expires_delta or self.expires_delta)
        to_encode.update({"exp": expire})
        return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)

    def __call__(self, token: str = Depends(OAuth2PasswordBearer(tokenUrl="token"))) -> str:
        """
        Dependency that:
          1. Extracts the Bearer token from Authorization header
          2. Decodes & validates it
          3. Returns the username (from 'sub' claim) or raises 401
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            username: str = payload.get("sub")
            if not username:
                raise ValueError("Missing sub")
            return username
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
