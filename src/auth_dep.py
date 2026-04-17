from dataclasses import dataclass

import httpx
from fastapi import Header, HTTPException, status

from src.config import settings


@dataclass(frozen=True)
class TokenPayload:
    sub: str
    role: str
    email: str
    org: str


async def require_user(authorization: str | None = Header(None)) -> TokenPayload:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
        )
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"{settings.AUTH_SERVICE_URL.rstrip('/')}/api/auth/introspect",
            headers={"Authorization": authorization},
        )
    if response.status_code != status.HTTP_200_OK:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )
    data = response.json()
    return TokenPayload(
        sub=str(data["sub"]),
        role=str(data["role"]),
        email=str(data["email"]),
        org=str(data["org"]),
    )
