package mk.ukim.finki.das.cryptoproject.web;

import lombok.AllArgsConstructor;
import mk.ukim.finki.das.cryptoproject.model.enums.Role;
import mk.ukim.finki.das.cryptoproject.service.UserService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping("/register")
@AllArgsConstructor
public class RegisterController {

    private final UserService userService;

    @GetMapping
    public String getRegisterPage(Model model) {
        return "register";
    }

    @PostMapping
    public String register(@RequestParam String name,
                           @RequestParam String surname,
                           @RequestParam String username,
                           @RequestParam String password,
                           @RequestParam String repeatPassword,
                           Model model) {

        try {
            this.userService.register(
                    username, password, repeatPassword, name, surname, Role.ROLE_USER);
            return "redirect:/login";
        } catch (RuntimeException e) {
            model.addAttribute("error", e.getMessage());
            return "register";
        }
    }
}

