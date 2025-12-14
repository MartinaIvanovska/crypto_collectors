package mk.ukim.finki.das.cryptoproject.service;

import mk.ukim.finki.das.cryptoproject.model.User;
import mk.ukim.finki.das.cryptoproject.model.exceptions.InvalidArgumentsException;
import mk.ukim.finki.das.cryptoproject.model.exceptions.InvalidUserCredentialsException;
import mk.ukim.finki.das.cryptoproject.repository.UserRepository;
import org.springframework.stereotype.Service;

@Service
public class AuthServiceImpl implements AuthService {
    private final UserRepository userRepository;

    public AuthServiceImpl(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public User login(String username, String password) {
        if (username == null || username.isEmpty() || password == null || password.isEmpty()) {
            throw new InvalidArgumentsException();
        }

        return this.userRepository.findByUsernameAndPassword(username, password)
                .orElseThrow(InvalidUserCredentialsException::new);
    }
}

