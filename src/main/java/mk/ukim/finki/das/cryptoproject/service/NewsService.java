package mk.ukim.finki.das.cryptoproject.service;

import mk.ukim.finki.das.cryptoproject.model.News;
import mk.ukim.finki.das.cryptoproject.repository.NewsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class NewsService {

    @Autowired
    private NewsRepository newsRepository;

    public Page<News> getAllNews(Pageable pageable) {
        return newsRepository.findAllByOrderByPublishedAtDesc(pageable);
    }
}
