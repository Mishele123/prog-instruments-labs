import pytest

from main import User, UserService


def test_add_user():
    service = UserService("test_users.json")
    service.users = []
    service.add_user("john_doe", "john@example.com")

    assert len(service.users) == 1
    assert service.users[0].username == "john_doe"
    assert service.users[0].email == "john@example.com"


def test_get_user():
    service = UserService("test_users.json")
    service.users = [User(1, "john_doe", "john@example.com")]

    user = service.get_user(1)
    
    assert user is not None
    assert user.username == "john_doe"


def test_remove_user():
    service = UserService("test_users.json")
    service.users = [User(1, "john_doe", "john@example.com")]

    service.remove_user(1)
    
    assert len(service.users) == 0


def test_update_user():
    service = UserService("test_users.json")
    service.users = [User(1, "john_doe", "john@example.com")]

    service.update_user(1, email="john_new@example.com")
    
    assert service.users[0].email == "john_new@example.com"



