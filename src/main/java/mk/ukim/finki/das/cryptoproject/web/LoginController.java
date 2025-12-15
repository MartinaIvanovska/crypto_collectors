package mk.ukim.finki.das.cryptoproject.web;

import jakarta.servlet.http.HttpServletRequest;
import mk.ukim.finki.das.cryptoproject.model.User;
import mk.ukim.finki.das.cryptoproject.service.AuthService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping("/login")
public class LoginController {
    private final AuthService authService;

    public LoginController(AuthService authService) {
        this.authService = authService;
    }

    @GetMapping
    public String getLoginPage(Model model) {
        return "login";
    }

    @PostMapping
    public String login(@RequestParam String username,
                        @RequestParam String password,
                        Model model,
                        HttpServletRequest req) {
        User user;

        try {
            user = this.authService.login(username, password);
            req.getSession().setAttribute("user", user);
            return "redirect:/cryptos";
        } catch (RuntimeException e) {
            model.addAttribute("error", e.getMessage());
            model.addAttribute("bodyContent", "login");
            return "login";
        }
    }
}

