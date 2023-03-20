package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
	"jordwyatt.github.com/gmail-janitor/oauth"
)

var MAX_THREADS_PER_RUN = 10000
var RATE_LIMIT_SLEEP = 25 * time.Millisecond

type shouldArchiveResult struct {
	threadId      string
	shouldArchive bool
	err           error
}

func main() {
	srv := getGmailService()
	cleanup(srv)
}

func getGmailService() *gmail.Service {
	ctx := context.Background()
	b, err := os.ReadFile("credentials.json")
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b, gmail.GmailModifyScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := oauth.GetClient(config)

	srv, err := gmail.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve Gmail client: %v", err)
	}

	return srv
}

func cleanup(srv *gmail.Service) {
	user := "me"

	threads := getAllThreads(user, srv)
	idsOfThreadsToArchive := getThreadsToArchive(threads, user, srv)

	fmt.Printf("%v threads to archive out of %v\n", len(idsOfThreadsToArchive), len(threads))

	archiveThreads(idsOfThreadsToArchive, user, srv)
}

func getThreads(srv *gmail.Service, user string, nextToken string) ([]*gmail.Thread, string) {
	fmt.Printf("Fetching threads using next token: %v\n", nextToken)
	t, err := srv.Users.Threads.List(user).LabelIds("INBOX").PageToken(nextToken).Do()

	if err != nil {
		log.Fatalf("Unable to retrieve threads: %v", err)
	}

	if len(t.Threads) == 0 {
		fmt.Println("No threads found.")
	} else {
		fmt.Println("Fetched " + fmt.Sprint(len(t.Threads)) + " threads from page.")
	}

	return t.Threads, t.NextPageToken
}

func msToTime(ms int64) time.Time {
	return time.Unix(0, ms*int64(time.Millisecond))
}

func getCutoffDate() time.Time {
	delayDays := 2
	now := time.Now()
	then := now.Add(time.Duration(-delayDays) * (time.Hour * 24))
	return then
}

func getAllThreads(user string, srv *gmail.Service) []*gmail.Thread {
	nextToken := ""
	allThreads := []*gmail.Thread{}

	for {
		threads, nextPageToken := getThreads(srv, user, nextToken)
		allThreads = append(allThreads, threads...)

		if nextPageToken == "" || len(allThreads) > MAX_THREADS_PER_RUN {
			break
		}

		nextToken = nextPageToken
	}

	fmt.Println("Fetched " + fmt.Sprint(len(allThreads)) + " threads in total")

	return allThreads
}

func getThreadsToArchive(threads []*gmail.Thread, user string, srv *gmail.Service) []string {
	threadsIds := []string{}
	ch := make(chan shouldArchiveResult, len(threads))
	cutOffDate := getCutoffDate()
	fmt.Printf("Filtering %v threads by date. Threads with latest message date before %s will be archived.\n", len(threads), cutOffDate.String())

	var wg sync.WaitGroup

	for _, t := range threads {
		wg.Add(1)
		t := t
		// Avoid rate limiting
		time.Sleep(RATE_LIMIT_SLEEP)

		go func() {
			defer wg.Done()
			shouldArchive(t.Id, user, cutOffDate, srv, ch)
		}()
	}

	wg.Wait()
	close(ch)

	for result := range ch {
		if result.shouldArchive {
			threadsIds = append(threadsIds, result.threadId)
		}
	}

	fmt.Printf("Completed filtering. %v threads will be archived\n", len(threadsIds))

	return threadsIds
}

func shouldArchive(threadId string, user string, cutoffDate time.Time, srv *gmail.Service, ch chan<- shouldArchiveResult) {
	threadDetails, err := srv.Users.Threads.Get(user, threadId).Do()

	if err != nil {
		log.Fatalf("Unable to GET thread with ID: %v\n %v", threadId, err)
		ch <- shouldArchiveResult{threadId, false, err}
	}

	lastMessage := threadDetails.Messages[len(threadDetails.Messages)-1]
	lastMessageDateReceived := msToTime(lastMessage.InternalDate)
	containsStarredMessage := doesThreadContainStarredMessage(threadDetails)
	shouldArchive := lastMessageDateReceived.Before(cutoffDate) && !containsStarredMessage

	if shouldArchive {
		ch <- shouldArchiveResult{threadId, true, nil}
	} else {
		ch <- shouldArchiveResult{threadId, false, nil}
	}
}

func archiveThreads(threadsIds []string, user string, srv *gmail.Service) {
	var wg sync.WaitGroup

	for _, threadId := range threadsIds {
		wg.Add(1)

		threadId := threadId

		// Avoid rate limiting
		time.Sleep(RATE_LIMIT_SLEEP)

		go func() {
			defer wg.Done()
			archiveThread(threadId, user, srv)
		}()
	}

	wg.Wait()
}

func archiveThread(threadId string, user string, srv *gmail.Service) {
	fmt.Println("Archiving thread: " + threadId)
	_, err := srv.Users.Threads.Modify(user, threadId, &gmail.ModifyThreadRequest{RemoveLabelIds: []string{"INBOX"}}).Do()
	if err != nil {
		log.Fatalf("Unable to ARCHIVE thread with ID: %v\n %v", threadId, err)
	}
	fmt.Println("Archived thread: " + threadId)
}

func doesThreadContainStarredMessage(thread *gmail.Thread) bool {
	for _, message := range thread.Messages {
		for _, label := range message.LabelIds {
			if label == "STARRED" {
				return true
			}
		}
	}

	return false
}
