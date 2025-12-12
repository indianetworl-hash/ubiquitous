"""
Ultimate Complete Full-Featured Application Module
A comprehensive production-ready application with all components integrated.
Created: 2025-12-12 11:28:45 UTC
Author: indianetworl-hash
"""

import os
import sys
import json
import logging
import asyncio
import sqlite3
from typing import Dict, List, Any, Optional, Tuple, Callable, Union
from datetime import datetime, timedelta
from functools import wraps
from dataclasses import dataclass, asdict, field
from enum import Enum
from abc import ABC, abstractmethod
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from fastapi import FastAPI, HTTPException, Depends, Header, Query, Body, status
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZIPMiddleware
from fastapi.security import HTTPBearer, HTTPAuthCredentials

import uvicorn
import pydantic
from pydantic import BaseModel, Field, validator, root_validator
import jwt
from jwt.exceptions import InvalidTokenError

# ============================================================================
# CONFIGURATION AND ENVIRONMENT SETUP
# ============================================================================

class Config:
    """Central configuration management for the application"""
    
    # Application Settings
    APP_NAME = "Ultimate Complete Application"
    APP_VERSION = "1.0.0"
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
    
    # Server Settings
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", 8000))
    WORKERS = int(os.getenv("WORKERS", 4))
    
    # Security Settings
    SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
    ALGORITHM = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES = 30
    REFRESH_TOKEN_EXPIRE_DAYS = 7
    
    # Database Settings
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///app.db")
    DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/app.db")
    DB_POOL_SIZE = 5
    DB_MAX_OVERFLOW = 10
    
    # API Settings
    API_PREFIX = "/api/v1"
    CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")
    
    # External APIs
    EXTERNAL_API_TIMEOUT = 30
    EXTERNAL_API_RETRIES = 3
    EXTERNAL_API_BASE_URL = os.getenv("EXTERNAL_API_BASE_URL", "https://api.example.com")
    EXTERNAL_API_KEY = os.getenv("EXTERNAL_API_KEY", "")
    
    # Logging Settings
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_FILE = os.getenv("LOG_FILE", "app.log")
    
    # Cache Settings
    CACHE_ENABLED = True
    CACHE_TTL = 300  # 5 minutes
    
    # Rate Limiting
    RATE_LIMIT_ENABLED = True
    RATE_LIMIT_REQUESTS = 100
    RATE_LIMIT_PERIOD = 60  # seconds
    
    @classmethod
    def get_config(cls) -> Dict[str, Any]:
        """Get all configuration as dictionary"""
        return {
            attr: getattr(cls, attr)
            for attr in dir(cls)
            if not attr.startswith('_') and attr.isupper()
        }


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging() -> logging.Logger:
    """Configure application logging"""
    logger = logging.getLogger("app")
    logger.setLevel(getattr(logging, Config.LOG_LEVEL))
    
    # File Handler
    file_handler = logging.FileHandler(Config.LOG_FILE)
    file_handler.setLevel(getattr(logging, Config.LOG_LEVEL))
    
    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, Config.LOG_LEVEL))
    
    # Formatter
    formatter = logging.Formatter(Config.LOG_FORMAT)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


logger = setup_logging()


# ============================================================================
# ENUMERATIONS
# ============================================================================

class UserRole(str, Enum):
    """User role enumeration"""
    ADMIN = "admin"
    MODERATOR = "moderator"
    USER = "user"
    GUEST = "guest"


class RequestStatus(str, Enum):
    """Request status enumeration"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ErrorCode(str, Enum):
    """Error code enumeration"""
    INVALID_REQUEST = "INVALID_REQUEST"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    EXTERNAL_API_ERROR = "EXTERNAL_API_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"


# ============================================================================
# DATA MODELS
# ============================================================================

class PaginationParams(BaseModel):
    """Pagination parameters"""
    skip: int = Field(0, ge=0)
    limit: int = Field(10, ge=1, le=100)
    sort_by: Optional[str] = None
    sort_order: str = Field("asc", regex="^(asc|desc)$")


class ErrorResponse(BaseModel):
    """Standard error response model"""
    status: str = "error"
    code: str
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SuccessResponse(BaseModel):
    """Standard success response model"""
    status: str = "success"
    data: Any = None
    message: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


@dataclass
class User:
    """User data model"""
    id: int
    username: str
    email: str
    password_hash: str
    role: UserRole = UserRole.USER
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        data = asdict(self)
        data['role'] = self.role.value
        return data


@dataclass
class RequestLog:
    """Request logging model"""
    id: int
    user_id: Optional[int]
    method: str
    path: str
    status_code: int
    response_time: float
    created_at: datetime = field(default_factory=datetime.utcnow)


class LoginRequest(BaseModel):
    """Login request model"""
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6)


class TokenResponse(BaseModel):
    """Token response model"""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class UserCreateRequest(BaseModel):
    """User creation request model"""
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., regex=r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
    password: str = Field(..., min_length=6)
    role: UserRole = UserRole.USER


class UserResponse(BaseModel):
    """User response model"""
    id: int
    username: str
    email: str
    role: str
    is_active: bool
    created_at: datetime


class ItemModel(BaseModel):
    """Item model"""
    id: Optional[int] = None
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    price: float = Field(..., gt=0)
    quantity: int = Field(..., ge=0)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ExternalAPIResponse(BaseModel):
    """External API response model"""
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    code: Optional[int] = None


# ============================================================================
# DECORATORS
# ============================================================================

def rate_limit(requests_limit: int = None, period: int = None):
    """Rate limiting decorator"""
    limit = requests_limit or Config.RATE_LIMIT_REQUESTS
    time_period = period or Config.RATE_LIMIT_PERIOD
    request_times = {}
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not Config.RATE_LIMIT_ENABLED:
                return await func(*args, **kwargs)
            
            current_time = datetime.utcnow()
            func_name = func.__name__
            
            if func_name not in request_times:
                request_times[func_name] = []
            
            # Remove old requests outside the time period
            request_times[func_name] = [
                t for t in request_times[func_name]
                if (current_time - t).total_seconds() < time_period
            ]
            
            # Check if limit exceeded
            if len(request_times[func_name]) >= limit:
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded"
                )
            
            request_times[func_name].append(current_time)
            return await func(*args, **kwargs)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not Config.RATE_LIMIT_ENABLED:
                return func(*args, **kwargs)
            
            current_time = datetime.utcnow()
            func_name = func.__name__
            
            if func_name not in request_times:
                request_times[func_name] = []
            
            request_times[func_name] = [
                t for t in request_times[func_name]
                if (current_time - t).total_seconds() < time_period
            ]
            
            if len(request_times[func_name]) >= limit:
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded"
                )
            
            request_times[func_name].append(current_time)
            return func(*args, **kwargs)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    
    return decorator


def cache_result(ttl: int = None):
    """Caching decorator"""
    cache_time = ttl or Config.CACHE_TTL
    cache_store = {}
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not Config.CACHE_ENABLED:
                return await func(*args, **kwargs)
            
            cache_key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            current_time = datetime.utcnow()
            
            if cache_key in cache_store:
                cached_result, cached_time = cache_store[cache_key]
                if (current_time - cached_time).total_seconds() < cache_time:
                    logger.debug(f"Cache hit for {func.__name__}")
                    return cached_result
            
            result = await func(*args, **kwargs)
            cache_store[cache_key] = (result, current_time)
            return result
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not Config.CACHE_ENABLED:
                return func(*args, **kwargs)
            
            cache_key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            current_time = datetime.utcnow()
            
            if cache_key in cache_store:
                cached_result, cached_time = cache_store[cache_key]
                if (current_time - cached_time).total_seconds() < cache_time:
                    logger.debug(f"Cache hit for {func.__name__}")
                    return cached_result
            
            result = func(*args, **kwargs)
            cache_store[cache_key] = (result, current_time)
            return result
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    
    return decorator


def require_role(*roles: UserRole):
    """Require specific user role decorator"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(current_user: User = Depends(get_current_user), *args, **kwargs):
            if current_user.role not in roles:
                raise HTTPException(
                    status_code=403,
                    detail="Insufficient permissions"
                )
            return await func(current_user, *args, **kwargs)
        
        return wrapper
    
    return decorator


def handle_exceptions(func: Callable) -> Callable:
    """Exception handling decorator"""
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail="Internal server error"
            )
    
    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail="Internal server error"
            )
    
    return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

class DatabaseManager:
    """Database management class"""
    
    def __init__(self, db_path: str = Config.DATABASE_PATH):
        """Initialize database manager"""
        self.db_path = db_path
        self._ensure_database_exists()
        self._init_tables()
    
    def _ensure_database_exists(self) -> None:
        """Ensure database file exists"""
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
    
    def _init_tables(self) -> None:
        """Initialize database tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    role TEXT DEFAULT 'user',
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Items table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    price REAL NOT NULL,
                    quantity INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Request logs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS request_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    response_time REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            """)
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    def get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def execute(self, query: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute database query"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor
        except sqlite3.Error as e:
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            conn.close()
    
    def fetch_one(self, query: str, params: tuple = ()) -> Optional[Dict]:
        """Fetch single row"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            conn.close()
    
    def fetch_all(self, query: str, params: tuple = ()) -> List[Dict]:
        """Fetch multiple rows"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            conn.close()
    
    def insert_user(self, username: str, email: str, password_hash: str,
                   role: str = "user") -> int:
        """Insert user"""
        cursor = self.execute(
            """
            INSERT INTO users (username, email, password_hash, role)
            VALUES (?, ?, ?, ?)
            """,
            (username, email, password_hash, role)
        )
        return cursor.lastrowid
    
    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Get user by username"""
        return self.fetch_one(
            "SELECT * FROM users WHERE username = ?",
            (username,)
        )
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Get user by ID"""
        return self.fetch_one(
            "SELECT * FROM users WHERE id = ?",
            (user_id,)
        )
    
    def insert_item(self, name: str, price: float, description: str = None,
                   quantity: int = 0) -> int:
        """Insert item"""
        cursor = self.execute(
            """
            INSERT INTO items (name, description, price, quantity)
            VALUES (?, ?, ?, ?)
            """,
            (name, description, price, quantity)
        )
        return cursor.lastrowid
    
    def get_item_by_id(self, item_id: int) -> Optional[Dict]:
        """Get item by ID"""
        return self.fetch_one(
            "SELECT * FROM items WHERE id = ?",
            (item_id,)
        )
    
    def get_all_items(self, skip: int = 0, limit: int = 10) -> List[Dict]:
        """Get all items with pagination"""
        return self.fetch_all(
            "SELECT * FROM items LIMIT ? OFFSET ?",
            (limit, skip)
        )
    
    def update_item(self, item_id: int, name: str = None, description: str = None,
                   price: float = None, quantity: int = None) -> bool:
        """Update item"""
        updates = []
        params = []
        
        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if description is not None:
            updates.append("description = ?")
            params.append(description)
        if price is not None:
            updates.append("price = ?")
            params.append(price)
        if quantity is not None:
            updates.append("quantity = ?")
            params.append(quantity)
        
        if not updates:
            return False
        
        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(item_id)
        
        query = f"UPDATE items SET {', '.join(updates)} WHERE id = ?"
        self.execute(query, tuple(params))
        return True
    
    def delete_item(self, item_id: int) -> bool:
        """Delete item"""
        self.execute("DELETE FROM items WHERE id = ?", (item_id,))
        return True
    
    def log_request(self, user_id: Optional[int], method: str, path: str,
                   status_code: int, response_time: float) -> int:
        """Log request"""
        cursor = self.execute(
            """
            INSERT INTO request_logs (user_id, method, path, status_code, response_time)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, method, path, status_code, response_time)
        )
        return cursor.lastrowid


# ============================================================================
# EXTERNAL API INTEGRATION
# ============================================================================

class ExternalAPIClient:
    """External API client"""
    
    def __init__(self, base_url: str = Config.EXTERNAL_API_BASE_URL,
                 api_key: str = Config.EXTERNAL_API_KEY,
                 timeout: int = Config.EXTERNAL_API_TIMEOUT):
        """Initialize API client"""
        self.base_url = base_url
        self.api_key = api_key
        self.timeout = timeout
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=Config.EXTERNAL_API_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers"""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": f"{Config.APP_NAME}/{Config.APP_VERSION}"
        }
    
    async def get(self, endpoint: str, params: Dict = None) -> ExternalAPIResponse:
        """GET request"""
        try:
            url = f"{self.base_url}/{endpoint}"
            logger.info(f"External API GET request: {url}")
            
            response = self.session.get(
                url,
                headers=self._get_headers(),
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return ExternalAPIResponse(
                    success=True,
                    data=response.json(),
                    code=response.status_code
                )
            else:
                return ExternalAPIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}",
                    code=response.status_code
                )
        except requests.exceptions.RequestException as e:
            logger.error(f"External API error: {str(e)}")
            return ExternalAPIResponse(
                success=False,
                error=str(e)
            )
    
    async def post(self, endpoint: str, data: Dict) -> ExternalAPIResponse:
        """POST request"""
        try:
            url = f"{self.base_url}/{endpoint}"
            logger.info(f"External API POST request: {url}")
            
            response = self.session.post(
                url,
                headers=self._get_headers(),
                json=data,
                timeout=self.timeout
            )
            
            if response.status_code in [200, 201]:
                return ExternalAPIResponse(
                    success=True,
                    data=response.json(),
                    code=response.status_code
                )
            else:
                return ExternalAPIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}",
                    code=response.status_code
                )
        except requests.exceptions.RequestException as e:
            logger.error(f"External API error: {str(e)}")
            return ExternalAPIResponse(
                success=False,
                error=str(e)
            )
    
    async def put(self, endpoint: str, data: Dict) -> ExternalAPIResponse:
        """PUT request"""
        try:
            url = f"{self.base_url}/{endpoint}"
            logger.info(f"External API PUT request: {url}")
            
            response = self.session.put(
                url,
                headers=self._get_headers(),
                json=data,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return ExternalAPIResponse(
                    success=True,
                    data=response.json(),
                    code=response.status_code
                )
            else:
                return ExternalAPIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}",
                    code=response.status_code
                )
        except requests.exceptions.RequestException as e:
            logger.error(f"External API error: {str(e)}")
            return ExternalAPIResponse(
                success=False,
                error=str(e)
            )
    
    async def delete(self, endpoint: str) -> ExternalAPIResponse:
        """DELETE request"""
        try:
            url = f"{self.base_url}/{endpoint}"
            logger.info(f"External API DELETE request: {url}")
            
            response = self.session.delete(
                url,
                headers=self._get_headers(),
                timeout=self.timeout
            )
            
            if response.status_code in [200, 204]:
                return ExternalAPIResponse(
                    success=True,
                    code=response.status_code
                )
            else:
                return ExternalAPIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}",
                    code=response.status_code
                )
        except requests.exceptions.RequestException as e:
            logger.error(f"External API error: {str(e)}")
            return ExternalAPIResponse(
                success=False,
                error=str(e)
            )
    
    def close(self) -> None:
        """Close session"""
        self.session.close()


# ============================================================================
# AUTHENTICATION AND AUTHORIZATION
# ============================================================================

class AuthenticationManager:
    """Authentication and token management"""
    
    @staticmethod
    def hash_password(password: str) -> str:
        """Hash password (simplified - use bcrypt in production)"""
        import hashlib
        return hashlib.sha256(password.encode()).hexdigest()
    
    @staticmethod
    def verify_password(password: str, password_hash: str) -> bool:
        """Verify password"""
        return AuthenticationManager.hash_password(password) == password_hash
    
    @staticmethod
    def create_access_token(user_id: int, username: str,
                           expires_delta: timedelta = None) -> str:
        """Create access token"""
        if expires_delta is None:
            expires_delta = timedelta(minutes=Config.ACCESS_TOKEN_EXPIRE_MINUTES)
        
        expire = datetime.utcnow() + expires_delta
        payload = {
            "sub": user_id,
            "username": username,
            "exp": expire,
            "iat": datetime.utcnow()
        }
        
        return jwt.encode(payload, Config.SECRET_KEY, algorithm=Config.ALGORITHM)
    
    @staticmethod
    def create_refresh_token(user_id: int) -> str:
        """Create refresh token"""
        expire = datetime.utcnow() + timedelta(days=Config.REFRESH_TOKEN_EXPIRE_DAYS)
        payload = {
            "sub": user_id,
            "type": "refresh",
            "exp": expire,
            "iat": datetime.utcnow()
        }
        
        return jwt.encode(payload, Config.SECRET_KEY, algorithm=Config.ALGORITHM)
    
    @staticmethod
    def decode_token(token: str) -> Dict[str, Any]:
        """Decode and verify token"""
        try:
            payload = jwt.decode(token, Config.SECRET_KEY, algorithms=[Config.ALGORITHM])
            return payload
        except InvalidTokenError as e:
            logger.error(f"Token decode error: {str(e)}")
            raise


# ============================================================================
# DEPENDENCIES
# ============================================================================

# Initialize global instances
db = DatabaseManager()
api_client = ExternalAPIClient()
security = HTTPBearer()


async def get_current_user(credentials: HTTPAuthCredentials = Depends(security)) -> User:
    """Dependency to get current user from token"""
    token = credentials.credentials
    
    try:
        payload = AuthenticationManager.decode_token(token)
        user_id: int = payload.get("sub")
        
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        
        user_data = db.get_user_by_id(user_id)
        if user_data is None:
            raise HTTPException(status_code=401, detail="User not found")
        
        return User(
            id=user_data['id'],
            username=user_data['username'],
            email=user_data['email'],
            password_hash=user_data['password_hash'],
            role=UserRole(user_data['role']),
            is_active=bool(user_data['is_active']),
            created_at=datetime.fromisoformat(user_data['created_at']),
            updated_at=datetime.fromisoformat(user_data['updated_at'])
        )
    except (jwt.InvalidTokenError, ValueError) as e:
        logger.error(f"Authentication error: {str(e)}")
        raise HTTPException(status_code=401, detail="Invalid token")


async def get_pagination(skip: int = Query(0, ge=0),
                         limit: int = Query(10, ge=1, le=100)) -> PaginationParams:
    """Dependency for pagination"""
    return PaginationParams(skip=skip, limit=limit)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def create_success_response(data: Any = None, message: str = None) -> SuccessResponse:
    """Create success response"""
    return SuccessResponse(data=data, message=message)


def create_error_response(code: str, message: str, details: Dict = None) -> ErrorResponse:
    """Create error response"""
    return ErrorResponse(code=code, message=message, details=details)


def format_response(status: str, data: Any = None, message: str = None) -> Dict:
    """Format response"""
    return {
        "status": status,
        "data": data,
        "message": message,
        "timestamp": datetime.utcnow().isoformat()
    }


async def log_request_async(user_id: Optional[int], method: str, path: str,
                           status_code: int, response_time: float) -> None:
    """Log request asynchronously"""
    try:
        db.log_request(user_id, method, path, status_code, response_time)
    except Exception as e:
        logger.error(f"Error logging request: {str(e)}")


def validate_email(email: str) -> bool:
    """Validate email format"""
    import re
    pattern = r"^[^\s@]+@[^\s@]+\.[^\s@]+$"
    return bool(re.match(pattern, email))


def generate_unique_username(base_username: str) -> str:
    """Generate unique username"""
    import random
    username = base_username
    counter = 1
    while db.get_user_by_username(username):
        username = f"{base_username}_{counter}"
        counter += 1
    return username


# ============================================================================
# HANDLERS
# ============================================================================

class AuthHandler:
    """Authentication handler"""
    
    @staticmethod
    async def register(request: UserCreateRequest) -> TokenResponse:
        """Register new user"""
        # Check if user exists
        if db.get_user_by_username(request.username):
            raise HTTPException(
                status_code=409,
                detail="Username already exists"
            )
        
        if db.fetch_one("SELECT * FROM users WHERE email = ?", (request.email,)):
            raise HTTPException(
                status_code=409,
                detail="Email already exists"
            )
        
        # Hash password and insert user
        password_hash = AuthenticationManager.hash_password(request.password)
        user_id = db.insert_user(
            request.username,
            request.email,
            password_hash,
            request.role.value
        )
        
        # Create tokens
        access_token = AuthenticationManager.create_access_token(user_id, request.username)
        refresh_token = AuthenticationManager.create_refresh_token(user_id)
        
        logger.info(f"User registered: {request.username}")
        
        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=Config.ACCESS_TOKEN_EXPIRE_MINUTES * 60
        )
    
    @staticmethod
    async def login(request: LoginRequest) -> TokenResponse:
        """Login user"""
        user_data = db.get_user_by_username(request.username)
        
        if not user_data:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        
        if not AuthenticationManager.verify_password(request.password, user_data['password_hash']):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        
        # Create tokens
        access_token = AuthenticationManager.create_access_token(
            user_data['id'],
            user_data['username']
        )
        refresh_token = AuthenticationManager.create_refresh_token(user_data['id'])
        
        logger.info(f"User logged in: {request.username}")
        
        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=Config.ACCESS_TOKEN_EXPIRE_MINUTES * 60
        )


class ItemHandler:
    """Item handler"""
    
    @staticmethod
    async def create_item(item: ItemModel) -> ItemModel:
        """Create item"""
        item_id = db.insert_item(
            item.name,
            item.price,
            item.description,
            item.quantity
        )
        
        item_data = db.get_item_by_id(item_id)
        logger.info(f"Item created: {item_id}")
        
        return ItemModel(
            id=item_data['id'],
            name=item_data['name'],
            description=item_data['description'],
            price=item_data['price'],
            quantity=item_data['quantity'],
            created_at=datetime.fromisoformat(item_data['created_at']),
            updated_at=datetime.fromisoformat(item_data['updated_at'])
        )
    
    @staticmethod
    async def get_item(item_id: int) -> ItemModel:
        """Get item by ID"""
        item_data = db.get_item_by_id(item_id)
        
        if not item_data:
            raise HTTPException(status_code=404, detail="Item not found")
        
        return ItemModel(
            id=item_data['id'],
            name=item_data['name'],
            description=item_data['description'],
            price=item_data['price'],
            quantity=item_data['quantity'],
            created_at=datetime.fromisoformat(item_data['created_at']),
            updated_at=datetime.fromisoformat(item_data['updated_at'])
        )
    
    @staticmethod
    async def list_items(pagination: PaginationParams) -> List[ItemModel]:
        """List items"""
        items_data = db.get_all_items(pagination.skip, pagination.limit)
        
        items = [
            ItemModel(
                id=item['id'],
                name=item['name'],
                description=item['description'],
                price=item['price'],
                quantity=item['quantity'],
                created_at=datetime.fromisoformat(item['created_at']),
                updated_at=datetime.fromisoformat(item['updated_at'])
            )
            for item in items_data
        ]
        
        return items
    
    @staticmethod
    async def update_item(item_id: int, item_update: ItemModel) -> ItemModel:
        """Update item"""
        if not db.get_item_by_id(item_id):
            raise HTTPException(status_code=404, detail="Item not found")
        
        db.update_item(
            item_id,
            item_update.name,
            item_update.description,
            item_update.price,
            item_update.quantity
        )
        
        logger.info(f"Item updated: {item_id}")
        
        item_data = db.get_item_by_id(item_id)
        return ItemModel(
            id=item_data['id'],
            name=item_data['name'],
            description=item_data['description'],
            price=item_data['price'],
            quantity=item_data['quantity'],
            created_at=datetime.fromisoformat(item_data['created_at']),
            updated_at=datetime.fromisoformat(item_data['updated_at'])
        )
    
    @staticmethod
    async def delete_item(item_id: int) -> Dict[str, str]:
        """Delete item"""
        if not db.get_item_by_id(item_id):
            raise HTTPException(status_code=404, detail="Item not found")
        
        db.delete_item(item_id)
        logger.info(f"Item deleted: {item_id}")
        
        return {"message": "Item deleted successfully"}


# ============================================================================
# ROUTER SETUP
# ============================================================================

app = FastAPI(
    title=Config.APP_NAME,
    version=Config.APP_VERSION,
    description="A comprehensive full-featured application",
    debug=Config.DEBUG
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.add_middleware(GZIPMiddleware, minimum_size=1000)


# ============================================================================
# ROUTES - HEALTH AND INFO
# ============================================================================

@app.get("/health", tags=["Health"])
@handle_exceptions
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": Config.APP_VERSION
    }


@app.get("/info", tags=["Info"])
@handle_exceptions
async def app_info():
    """Application info endpoint"""
    return {
        "name": Config.APP_NAME,
        "version": Config.APP_VERSION,
        "environment": Config.ENVIRONMENT,
        "debug": Config.DEBUG
    }


# ============================================================================
# ROUTES - AUTHENTICATION
# ============================================================================

@app.post("/api/v1/auth/register", response_model=TokenResponse, tags=["Authentication"])
@rate_limit(requests_limit=5, period=300)
@handle_exceptions
async def register(request: UserCreateRequest):
    """Register new user"""
    return await AuthHandler.register(request)


@app.post("/api/v1/auth/login", response_model=TokenResponse, tags=["Authentication"])
@rate_limit(requests_limit=10, period=60)
@handle_exceptions
async def login(request: LoginRequest):
    """Login user"""
    return await AuthHandler.login(request)


@app.post("/api/v1/auth/refresh", response_model=TokenResponse, tags=["Authentication"])
@handle_exceptions
async def refresh_token(current_user: User = Depends(get_current_user)):
    """Refresh access token"""
    access_token = AuthenticationManager.create_access_token(
        current_user.id,
        current_user.username
    )
    refresh_token = AuthenticationManager.create_refresh_token(current_user.id)
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=Config.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )


# ============================================================================
# ROUTES - USER
# ============================================================================

@app.get("/api/v1/users/me", response_model=UserResponse, tags=["Users"])
@handle_exceptions
async def get_current_user_info(current_user: User = Depends(get_current_user)):
    """Get current user information"""
    return UserResponse(
        id=current_user.id,
        username=current_user.username,
        email=current_user.email,
        role=current_user.role.value,
        is_active=current_user.is_active,
        created_at=current_user.created_at
    )


# ============================================================================
# ROUTES - ITEMS
# ============================================================================

@app.post("/api/v1/items", response_model=ItemModel, tags=["Items"])
@handle_exceptions
async def create_item(
    item: ItemModel,
    current_user: User = Depends(get_current_user)
):
    """Create new item"""
    if current_user.role not in [UserRole.ADMIN, UserRole.MODERATOR]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    return await ItemHandler.create_item(item)


@app.get("/api/v1/items/{item_id}", response_model=ItemModel, tags=["Items"])
@cache_result(ttl=300)
@handle_exceptions
async def get_item(item_id: int):
    """Get item by ID"""
    return await ItemHandler.get_item(item_id)


@app.get("/api/v1/items", response_model=List[ItemModel], tags=["Items"])
@cache_result(ttl=300)
@handle_exceptions
async def list_items(pagination: PaginationParams = Depends(get_pagination)):
    """List all items"""
    return await ItemHandler.list_items(pagination)


@app.put("/api/v1/items/{item_id}", response_model=ItemModel, tags=["Items"])
@handle_exceptions
async def update_item(
    item_id: int,
    item_update: ItemModel,
    current_user: User = Depends(get_current_user)
):
    """Update item"""
    if current_user.role not in [UserRole.ADMIN, UserRole.MODERATOR]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    return await ItemHandler.update_item(item_id, item_update)


@app.delete("/api/v1/items/{item_id}", tags=["Items"])
@handle_exceptions
async def delete_item(
    item_id: int,
    current_user: User = Depends(get_current_user)
):
    """Delete item"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    return await ItemHandler.delete_item(item_id)


# ============================================================================
# ROUTES - EXTERNAL API
# ============================================================================

@app.get("/api/v1/external/data", tags=["External API"])
@cache_result(ttl=600)
@handle_exceptions
async def fetch_external_data(
    endpoint: str = Query(..., description="API endpoint"),
    current_user: User = Depends(get_current_user)
):
    """Fetch data from external API"""
    result = await api_client.get(endpoint)
    
    if not result.success:
        raise HTTPException(
            status_code=502,
            detail=result.error or "External API error"
        )
    
    return result.data


@app.post("/api/v1/external/push", tags=["External API"])
@handle_exceptions
async def push_to_external_api(
    endpoint: str = Query(..., description="API endpoint"),
    data: Dict = Body(...),
    current_user: User = Depends(get_current_user)
):
    """Push data to external API"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    result = await api_client.post(endpoint, data)
    
    if not result.success:
        raise HTTPException(
            status_code=502,
            detail=result.error or "External API error"
        )
    
    return result.data


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "code": "HTTP_ERROR",
            "message": exc.detail,
            "timestamp": datetime.utcnow().isoformat()
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions"""
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "code": "INTERNAL_ERROR",
            "message": "Internal server error",
            "timestamp": datetime.utcnow().isoformat()
        }
    )


# ============================================================================
# STARTUP AND SHUTDOWN EVENTS
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Startup event"""
    logger.info(f"Starting {Config.APP_NAME} v{Config.APP_VERSION}")
    logger.info(f"Environment: {Config.ENVIRONMENT}")
    logger.info(f"Debug: {Config.DEBUG}")
    logger.info("Application started successfully")


@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown event"""
    logger.info("Shutting down application")
    api_client.close()
    logger.info("Application shutdown complete")


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main entry point"""
    logger.info(f"Starting {Config.APP_NAME} on {Config.HOST}:{Config.PORT}")
    
    uvicorn.run(
        "ultimate_complete_full:app",
        host=Config.HOST,
        port=Config.PORT,
        workers=Config.WORKERS if not Config.DEBUG else 1,
        log_level=Config.LOG_LEVEL.lower(),
        reload=Config.DEBUG,
        access_log=True
    )


if __name__ == "__main__":
    main()
