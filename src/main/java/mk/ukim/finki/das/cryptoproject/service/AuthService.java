package mk.ukim.finki.das.cryptoproject.service;

import mk.ukim.finki.das.cryptoproject.model.User;

public interface AuthService {
    User login(String username, String password);
}

