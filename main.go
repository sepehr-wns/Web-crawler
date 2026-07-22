package main

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/chromedp"
)

type UrlName struct {
	Url   string
	Depth int // for how much we wnat to go in
	Retry int // for retrying incase of emergency
}

type Result struct {
	Url       string
	Link      []string //for saving infos in slice
	Title     string   // for saving infos with this name
	Error     error
	Timespent time.Duration // for knowig how much time we spent
	Retry     int
}
type stats struct {
	Mu         sync.Mutex
	Pagenum    int
	Pagestack  int // stack for getting infos in order
	Errors     int
	Timetotal  time.Duration
	Foundlink  int
	Coursepage int
	Retrypage  int
}

//whenever we visit a page we gonna call method bellow to save infos

func (s *stats) Addvisit(Timespent time.Duration, Foundlink int, errors bool, Course bool) {
	s.Mu.Lock() // locking to add data
	defer s.Mu.Unlock()

	s.Pagenum++
	s.Timetotal += Timespent
	s.Foundlink += Foundlink

	if Course {
		s.Coursepage++
	}
	if errors {
		s.Errors++
	}
}
func (s *stats) Stackedpage() {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Pagestack++
}

func (s *stats) Retryedpage() {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Retrypage++
}
func (s *stats) Print() {
	fmt.Println("\n ******* crawled info's")
	fmt.Printf("\n Visited pages : %d", s.Pagenum)
	fmt.Printf("\n Page num in stack : %d", s.Pagestack)
	fmt.Printf("\n Errors equered  : %d", s.Errors)
	fmt.Printf("\n Total course pages found  : %d", s.Coursepage)
	fmt.Printf("\n Total Links : %d", s.Foundlink)
	fmt.Printf("\n Total retried times  : %d", s.Retrypage)

}

type Visitedsites struct { // preventing from opening web page twice
	mu           sync.RWMutex
	Visit        map[string]bool
	Depthmax     int
	Workes       int             // for limiting server from doing more
	stats        *stats          // for having access to all datas which we gained
	Allowedhosts map[string]bool // setting limit for opening webpages
	Start        time.Time
	Sem          chan struct{}
}

// new function for adding new infos to the struct Visitedsites
func NewCoursecrawl(works, depthmax int) *Visitedsites {
	return &Visitedsites{
		Visit:        make(map[string]bool),
		Depthmax:     depthmax,
		Workes:       works,
		stats:        &stats{},
		Allowedhosts: make(map[string]bool),
		Start:        time.Now(),
		Sem:          make(chan struct{}, works),
	}
}
func (v *Visitedsites) Validones(urlstr string) bool { // for avoiding useless infos like jpgs and etc
	avoidsavings := []string{
		".png", ".jpg", ".mp4", ".mp3", ".js", ".html", ".css", ".jpeg", ".gif", ".svg",
		".pdf", ".zip", ".rar", ".map", ".txt", ".xml", ".webp",
	}
	Makelow := strings.ToLower(urlstr)
	for _, ext := range avoidsavings {
		if strings.HasSuffix(Makelow, ext) {
			return true
		}
	}
	return false
}

func (v *Visitedsites) Coursepagetrue(urlstr string) bool {
	Pattern := []string{
		"/course/", "/courses/",
	}
	Makelow := strings.ToLower(urlstr)
	if v.Validones(urlstr) {
		return false
	}
	for _, pattern := range Pattern {
		if strings.Contains(Makelow, pattern) {
			return true
		}
	}
	return false
}
func (v *Visitedsites) Cancrawl(urlstr string, depth int) bool {
	if v.Validones(urlstr) {
		return false
	}
	Skippingdomains := []string{
		"google.com", "instagram.com", "unpkg.com", "facebook.com", "cloudeflare.com", "youtube.com",
	}
	parsedurl, err := url.Parse(urlstr)
	if err == nil {
		parsedurl.Host = strings.ToLower(parsedurl.Host)
		for _, skipdomain := range Skippingdomains {
			if strings.Contains(parsedurl.Host, skipdomain) {
				return false
			}
		}

	}
	v.mu.RLock()
	visited := v.Visit[urlstr]
	defer v.mu.RUnlock()

	if depth > v.Depthmax {
		return false
	}
	if visited {
		return false
	}
	return true
}
func (v *Visitedsites) Marksite(urlstr string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.Visit[urlstr] = true
}
func (v *Visitedsites) isMarksite(urlstr string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.Visit[urlstr]
}
func (v *Visitedsites) isAllowedsite(urlstr string) bool {
	if len(v.Allowedhosts) == 0 {
		return true
	}
	parsedURL, err := url.Parse(urlstr)
	if err != nil {
		return false
	}
	return v.Allowedhosts[parsedURL.Host]
}

// the main worker of programm
func (v *Visitedsites) Worker(ctx context.Context, id int, urls <-chan UrlName, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for urlinfo := range urls {
		select {
		case v.Sem <- struct{}{}:
			v.Processurl(ctx, id, urlinfo, results) // done
			<-v.Sem
		case <-ctx.Done():
			return
		}
	}
}
func (v *Visitedsites) Processurl(ctx context.Context, id int, urlinfo UrlName, result chan<- Result) {
	if !v.Cancrawl(urlinfo.Url, urlinfo.Depth) {
		result <- Result{
			Url:   urlinfo.Url,
			Error: fmt.Errorf("couldnt crawl"),
		}
		return
	}
	v.Marksite(urlinfo.Url)
	chromectx, cancel := chromedp.NewContext(ctx)
	defer cancel()
	Start := time.Now()

	var HtmlSaver string
	var title string
	err := chromedp.Run(chromectx,
		chromedp.Navigate(urlinfo.Url),
		chromedp.WaitReady("body", chromedp.ByQuery),
		chromedp.Sleep(3*time.Second),
		chromedp.Title(&title),
		chromedp.OuterHTML("html", &HtmlSaver, chromedp.ByQuery),
	)
	fetchtime := time.Since(Start)
	RESULT := Result{
		Url:       urlinfo.Url,
		Timespent: fetchtime,
		Error:     err,
		Retry:     urlinfo.Retry,
	}

	if err == nil {
		RESULT.Link = v.extractCourseLinks(HtmlSaver, urlinfo.Url)
		RESULT.Title = v.Extractedtitle(HtmlSaver)
		result <- RESULT

	} else if urlinfo.Retry < 2 {
		v.stats.Retryedpage()
		fmt.Printf("Retrying (%d/2) : %s\n,", urlinfo.Retry, urlinfo.Url)
		select {
		case <-ctx.Done():
			return
		default:
			result <- Result{
				Url:   urlinfo.Url,
				Error: err,
			}
			time.Sleep(2 * time.Second)
			v.Processurl(ctx, id, UrlName{
				Url:   urlinfo.Url,
				Depth: urlinfo.Depth,
				Retry: urlinfo.Retry + 1,
			}, result)
		}
	} else {
		result <- RESULT // send it to the main results of search
	}

}

func (v *Visitedsites) Extractedtitle(html string) string { // Ai involved !
	re := regexp.MustCompile(`(?i)<title[^>]*>([^>]+)</title>`)
	match := re.FindStringSubmatch(html)

	if len(match) > 1 {
		title := strings.TrimSpace(match[1])
		title = strings.ReplaceAll(title, "&amp;", "&")
		title = strings.ReplaceAll(title, "&lt;", "<")
		title = strings.ReplaceAll(title, "&gt;", ">")
		title = strings.ReplaceAll(title, "&qout;", "\"")
		title = strings.ReplaceAll(title, "&#39;", "'")
		return title
	}
	return "no title"
}
func (v *Visitedsites) extractCourseLinks(html, baseURL string) []string {
	links := make([]string, 0)
	seen := make(map[string]bool)

	baseURLParsed, _ := url.Parse(baseURL)

	// Look for links in href attributes
	re := regexp.MustCompile(`(?i)href=["']([^"']+)["']`)
	matches := re.FindAllStringSubmatch(html, -1)

	for _, match := range matches {
		if len(match) > 1 {
			link := strings.TrimSpace(match[1])

			// Skip invalid or unwanted links
			if link == "" ||
				strings.HasPrefix(link, "javascript:") ||
				strings.HasPrefix(link, "mailto:") ||
				strings.HasPrefix(link, "tel:") ||
				strings.HasPrefix(link, "#") ||
				strings.HasPrefix(link, "data:") ||
				strings.HasPrefix(link, "ws:") ||
				strings.HasPrefix(link, "wss:") {
				continue
			}

			// Parse and resolve URLs
			linkURL, err := url.Parse(link)
			if err != nil {
				continue
			}

			if !linkURL.IsAbs() {
				linkURL = baseURLParsed.ResolveReference(linkURL)
			}

			// Remove fragment
			linkURL.Fragment = ""
			cleanLink := linkURL.String()

			// Only keep course-related pages from the same domain
			if strings.HasPrefix(cleanLink, "http") &&
				linkURL.Host == baseURLParsed.Host &&
				!v.Validones(cleanLink) &&
				!seen[cleanLink] {

				// Prioritize course pages
				if v.isAllowedsite(cleanLink) {
					seen[cleanLink] = true
					links = append(links, cleanLink)
				} else if strings.Contains(cleanLink, "/courses") ||
					strings.Contains(cleanLink, "/category") {
					// Also include category pages to discover more courses
					seen[cleanLink] = true
					links = append(links, cleanLink)
				}
			}
		}
	}

	return links
} // Ai out !
// start to crawl
func (v *Visitedsites) Crawl(ctx context.Context, starturl string) {
	if parsedurl, err := url.Parse(starturl); err == nil {
		v.Allowedhosts[parsedurl.Host] = true // cheking if its allowed to put in allowed hosts or not
	}
	urlstack := make(chan UrlName, 1000)
	results := make(chan Result, 1000)

	var wg sync.WaitGroup
	for i := 0; i < v.Workes; i++ {
		wg.Add(1)
		go v.Worker(ctx, i, urlstack, results, &wg) //urlstack here is the same urls channel!
	}
	procesdone := make(chan bool)
	go v.Processresults(ctx, urlstack, results, procesdone)

	urlstack <- UrlName{Url: starturl, Depth: 0, Retry: 0}
	v.stats.Stackedpage()
	select {
	case <-procesdone:
		fmt.Println("\n crawling completed!")
	case <-ctx.Done():
		fmt.Println("\n crawling stopeed!", ctx.Err())
	}
	close(urlstack)
	wg.Wait()
	close(results)
	v.stats.Print()
	fmt.Printf("\n total time : %v\n", time.Since(v.Start))
}
func (v *Visitedsites) Processresults(ctx context.Context, urlstack chan<- UrlName, results <-chan Result, done chan<- bool) {
	urldepth := make(map[string]int)
	Pending := 1
	for {
		select {
		case <-ctx.Done():
			done <- true
			return
		case result, ok := <-results:
			if !ok {
				done <- true
				return
			}
			Pending--
			iscourse := v.Coursepagetrue(result.Url)
			if result.Error != nil {
				if result.Retry >= 2 {
					fmt.Printf("failed: %s \n after: %d retries -%v\n", result.Url, result.Retry, result.Error)
				}
				if Pending == 0 {
					done <- true
					return
				}
				continue
			}
			if iscourse {
				fmt.Printf("	corse: %s\n", result.Url)
				if result.Title != "no title!" {
					fmt.Printf("	course: %s\n", result.Title)
				}
				if len(result.Link) > 0 {
					fmt.Printf("Found: %d related courses !\n", len(result.Link))
				}
			} else {
				fmt.Printf("category: %s %d coourses found \n", result.Url, len(result.Link))
			}
			var Currentdepth int
			Currentdepth = urldepth[result.Url]
			if Currentdepth < v.Depthmax {
				newlink := 0
				for _, link := range result.Link {
					if v.isAllowedsite(link) && !v.isMarksite(link) && v.Cancrawl(link, Currentdepth+1) {
						select {
						case <-ctx.Done():
							done <- true
							return
						case urlstack <- UrlName{Url: link, Depth: Currentdepth, Retry: 0}:
							urldepth[link] = Currentdepth + 1
							Pending++
							v.stats.Stackedpage()
							newlink++

						}
					}
				}
				if newlink > 0 && iscourse {
					fmt.Printf("	stacke: %d new course page \n", newlink)

				}
			}
			if Pending == 0 {
				done <- true
				return
			}
		}

	}
}
func (v *Visitedsites) Reporter(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second) // for setting time for report
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			v.stats.Print()
		}
	}
}
func main() {
	CRAWLER := NewCoursecrawl(4, 2)                                         // setting workes and limits
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // deadline for canceling
	defer cancel()

	starturl := "https://faradars.org/explore"
	CRAWLER.Crawl(ctx, starturl)
}
