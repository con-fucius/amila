
import pytest
import hmac
import hashlib
import time
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient
from app.core.security_middleware import HMACMiddleware
from app.core.config import settings

# Setup a minimal app for testing middleware
def create_test_app():
    app = FastAPI()
    
    # Add middleware
    app.add_middleware(HMACMiddleware)
    
    @app.get("/health")
    def health():
        return {"status": "ok"}
        
    @app.post("/api/v1/queries/submit")
    def protected_submit(data: dict):
        return {"status": "received", "data": data}
        
    @app.get("/api/v1/auth/login")
    def exempt_auth():
        return {"status": "exempt"}

    return app

client = TestClient(create_test_app())
SECRET = settings.hmac_secret_key

def generate_signature(method, path, timestamp, body=b"", secret=SECRET):
    payload = f"{method}{path}{timestamp}".encode('utf-8') + body
    return hmac.new(secret.encode('utf-8'), payload, hashlib.sha256).hexdigest()

def test_exempt_path():
    """Test that exempt paths do not require signature"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    
    response = client.get("/api/v1/auth/login")
    assert response.status_code == 200

def test_missing_headers_protected():
    """Test missing headers on protected path"""
    response = client.post("/api/v1/queries/submit", json={"query": "test"})
    # Should be 401 because headers are missing
    assert response.status_code == 401
    assert "Missing signature" in response.json()["detail"]

def test_valid_signature():
    """Test valid signature on protected path"""
    timestamp = str(int(time.time()))
    body = b'{"query": "test"}'
    path = "/api/v1/queries/submit"
    method = "POST"
    
    sig = generate_signature(method, path, timestamp, body)
    
    headers = {
        "X-Signature": sig,
        "X-Timestamp": timestamp
    }
    
    response = client.post(path, content=body, headers=headers)
    assert response.status_code == 200
    assert response.json()["status"] == "received"

def test_invalid_signature():
    """Test invalid signature"""
    timestamp = str(int(time.time()))
    body = b'{"query": "test"}'
    path = "/api/v1/queries/submit"
    
    headers = {
        "X-Signature": "invalid_signature_hex",
        "X-Timestamp": timestamp
    }
    
    response = client.post(path, content=body, headers=headers)
    assert response.status_code == 401
    assert "Invalid request" in response.json()["detail"]

def test_replay_attack():
    """Test expired timestamp"""
    # 10 minutes ago
    timestamp = str(int(time.time()) - 600)
    body = b'{"query": "test"}'
    path = "/api/v1/queries/submit"
    method = "POST"
    
    sig = generate_signature(method, path, timestamp, body)
    
    headers = {
        "X-Signature": sig,
        "X-Timestamp": timestamp
    }
    
    response = client.post(path, content=body, headers=headers)
    assert response.status_code == 401
    assert "expired" in response.json()["detail"]

def test_tampered_body():
    """Test valid signature but tampered body"""
    timestamp = str(int(time.time()))
    body = b'{"query": "test"}'
    tampered_body = b'{"query": "hacked"}'
    path = "/api/v1/queries/submit"
    method = "POST"
    
    # Sign original body
    sig = generate_signature(method, path, timestamp, body)
    
    headers = {
        "X-Signature": sig,
        "X-Timestamp": timestamp
    }
    
    # Send tampered body
    response = client.post(path, content=tampered_body, headers=headers)
    assert response.status_code == 401
    assert "Invalid request" in response.json()["detail"]
