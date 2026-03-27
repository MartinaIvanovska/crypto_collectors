package mk.ukim.finki.das.cryptoproject.web;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@RequestMapping({"/","/home"})
@Controller
public class HomeController {

    @GetMapping
    public String home(){
        return "home";
    }

    @GetMapping("/intro")
    public  String intro(){
        return "intro";
    }

    @GetMapping("/whales")
    public  String whales(){
        return "whales";
    }

}
