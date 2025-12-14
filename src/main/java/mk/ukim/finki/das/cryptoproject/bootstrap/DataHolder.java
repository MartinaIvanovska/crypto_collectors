package mk.ukim.finki.das.cryptoproject.bootstrap;

import jakarta.annotation.PostConstruct;
import mk.ukim.finki.das.cryptoproject.model.User;
import mk.ukim.finki.das.cryptoproject.model.enums.Role;
import mk.ukim.finki.das.cryptoproject.repository.UserRepository;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class DataHolder {

    public static List<User> users = null;


    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    public DataHolder(
            UserRepository userRepository,
            PasswordEncoder passwordEncoder
    ) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
    }

    @PostConstruct
    public void init() {

        if (userRepository.findAll().isEmpty()) {
            users = new ArrayList<>();
            users.add(new User("martina", passwordEncoder.encode("martina"), "Martina", "Ivanovska", Role.ROLE_USER));
            users.add(new User("admin", passwordEncoder.encode("admin"), "admin", "admin", Role.ROLE_ADMIN));
            userRepository.saveAll(users);
        }
    }
}


