"""Bearer token authentication middleware for context-helpers FastAPI app."""

import secrets

from fastapi import HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

_bearer_scheme = HTTPBearer(auto_error=False)


def make_auth_dependency(api_key: str):
    """Return a FastAPI dependency that validates Bearer tokens.

    Args:
        api_key: The expected API key value.

    Returns:
        A callable dependency that raises HTTP 401 on invalid/missing token.
    """

    async def _verify_token(request: Request) -> None:
        credentials: HTTPAuthorizationCredentials | None = await _bearer_scheme(request)
        if credentials is None or not secrets.compare_digest(credentials.credentials, api_key):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing Bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    return _verify_token
