package mk.ukim.finki.das.cryptoproject.web;

import mk.ukim.finki.das.cryptoproject.model.News;
import mk.ukim.finki.das.cryptoproject.service.NewsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.*;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@RequestMapping("/news")
@Controller
public class NewsController {

    @Autowired
    private NewsService newsService;

    @GetMapping
    public String listNews(
            @RequestParam(value = "pageNum", defaultValue = "1") int pageNum,
            @RequestParam(value = "pageSize", defaultValue = "20") int pageSize,
            Model model) {

        Pageable pageable = PageRequest.of(
                pageNum - 1,
                pageSize,
                Sort.by("publishedAt").descending()
        );

        Page<News> page = newsService.getAllNews(pageable);

        model.addAttribute("page", page);
        return "news";
    }
}
