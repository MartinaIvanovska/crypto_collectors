package mk.ukim.finki.das.cryptoproject.service;

import mk.ukim.finki.das.cryptoproject.model.User;
import mk.ukim.finki.das.cryptoproject.model.enums.Role;
import mk.ukim.finki.das.cryptoproject.model.exceptions.InvalidArgumentsException;
import mk.ukim.finki.das.cryptoproject.model.exceptions.PasswordsDoNotMatchException;
import mk.ukim.finki.das.cryptoproject.model.exceptions.UsernameAlreadyExistsException;
import mk.ukim.finki.das.cryptoproject.model.exceptions.UsernameNotFoundException;
import mk.ukim.finki.das.cryptoproject.repository.UserRepository;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
public class UserServiceImpl implements UserService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    public UserServiceImpl(UserRepository userRepository, PasswordEncoder passwordEncoder) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
    }

    @Override
    public User register(String username, String password, String repeatPassword, String name, String surname, Role role) {
        if (username == null || password == null || username.isEmpty() || password.isEmpty()) {
            throw new InvalidArgumentsException();
        }

        if (!password.equals(repeatPassword)) {
            throw new PasswordsDoNotMatchException();
        }

        if (this.userRepository.findByUsername(username).isPresent()) {
            throw new UsernameAlreadyExistsException(username);
        }

        User user = new User(username, passwordEncoder.encode(password), name, surname, role);

        return userRepository.save(user);
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        return userRepository.findByUsername(username).orElseThrow(() -> new UsernameNotFoundException(username));
    }
}
