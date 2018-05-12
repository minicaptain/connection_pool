package connection_pool

import (
	//"crypto/md5"
	"crypto/md5"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/spaolacci/murmur3"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Hash func(data []byte) uint32

type TTLValue struct {
	ErrorCount      int
	FirstOccurrence int64
	RequestCount    int
	Online          bool
}

type ServerPool struct {

	// mu protects candidate server list
	mu sync.Mutex

	// candidate server stores all candidate servers
	CandidateServer []string

	//candidate server status
	CandidateStatus map[string]*TTLValue

	//time interval, must supplied
	TimeInterval int64

	//consistent hash map
	consistentHashMap map[int]string

	//consistent hash keys
	consistentHashKeys []int

	hashFunc Hash

	Replicas int

	//current err rate when this value greater than MaxKickRate will
	//not kick down server util this value less than MaxKickRate
	errorRate float64

	//fail rate
	FailRate float64

	/*
	 *Kicked interval.
	 *after this interval the error server will be added back.
	 */
	KickedInterval time.Duration

	/*threshold for kick error server number
	 *
	 *value = len(down server) / len(total servers)
	 *
	 *default value is 1.0, that pool will kick all the servers
	 *when they are breaking down, which is a very important param,
	 *you should set it a proper value according to your cluster
	 *machine number.
	 */
	MaxKickRate float64
}

func NewServerPool(servers []string) *ServerPool {
	pool := &ServerPool{
		CandidateServer:    []string{},
		CandidateStatus:    map[string]*TTLValue{},
		Replicas:           1023,
		hashFunc:           murmur3.Sum32,
		consistentHashKeys: []int{},
		consistentHashMap:  map[int]string{},
		errorRate:          0.0,
		FailRate:           0.8,
		TimeInterval:       10,
		MaxKickRate:        1.0,
		KickedInterval:     time.Second * 20,
	}

	for _, server := range servers {
		pool.CandidateServer = append(pool.CandidateServer, server)
		pool.CandidateStatus[server] = &TTLValue{
			Online:       true,
			RequestCount: 0,
			ErrorCount:   0,
		}
	}
	pool.addServer(servers)
	fmt.Printf("[serverPool.NewServerPool]update the CadidateServer %v time %s \n", pool.CandidateServer, time.Now())
	return pool
}

func (p *ServerPool) addServer(keys []string) {
	for _, key := range keys {
		for i := 0; i < p.Replicas; i++ {
			md5Value := fmt.Sprintf("%x", md5.Sum([]byte(strconv.Itoa(i)+"_"+key)))
			hash := int(p.hashFunc([]byte(md5Value)))
			//hash := int(p.hashFunc([]byte(strconv.Itoa(i) + "_" + key)))
			p.consistentHashKeys = append(p.consistentHashKeys, hash)
			p.consistentHashMap[hash] = key
		}
	}
	sort.Ints(p.consistentHashKeys)
}

// GetServer returns header of servers list
func (p *ServerPool) GetServer() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	arrLen := len(p.CandidateServer)
	if arrLen > 0 {
		server := p.CandidateServer[rand.Intn(arrLen)]
		if p.CandidateStatus[server].FirstOccurrence+p.TimeInterval > time.Now().Unix() {
			p.CandidateStatus[server].RequestCount += 1
		} else {
			p.CandidateStatus[server].FirstOccurrence = time.Now().Unix()
			p.CandidateStatus[server].RequestCount = 1
			p.CandidateStatus[server].ErrorCount = 0
		}
		return server, nil
	}
	return "", errors.New("ServerPool: get server error")
}

//GetServer through consistent hash
func (p *ServerPool) GetServerFromConsistentHashAlgorithm(key string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.consistentHashMap) == 0 {
		return "", errors.New("ServerPool: get server error")
	}
	hash := int(p.hashFunc([]byte(key)))
	destKey := binarySearch(p.consistentHashKeys, hash)
	server := p.consistentHashMap[destKey]
	if p.CandidateStatus[server].FirstOccurrence+p.TimeInterval > time.Now().Unix() {
		p.CandidateStatus[server].RequestCount += 1
	} else {
		p.CandidateStatus[server].FirstOccurrence = time.Now().Unix()
		p.CandidateStatus[server].RequestCount = 1
		p.CandidateStatus[server].ErrorCount = 0
	}
	return server, nil
}

func binarySearch(keys []int, dest int) int {
	left := 0
	right := len(keys) - 1
	var mid int
	for left < right {
		mid = (left + right) >> 1
		if keys[mid] < dest {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if keys[right] >= dest {
		return keys[right]
	}
	return keys[0]
}

// ServerDown remove server from candidate list, and put back to end of server list
func (p *ServerPool) ServerDown(server string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v := p.CandidateStatus[server]
	v.ErrorCount++
	if time.Now().Unix() < v.FirstOccurrence+p.TimeInterval {
		rate := float64(v.ErrorCount) / float64(v.RequestCount)
		if rate > p.FailRate && p.errorRate <= p.MaxKickRate {
			p.CandidateStatus[server].Online = false
			log.Errorf("Server Down:%s \n", server)
			p.resetPoolServer()
			go func() {
				time.Sleep(p.KickedInterval)
				p.mu.Lock()
				defer p.mu.Unlock()
				p.CandidateStatus[server].Online = true
				p.resetPoolServer()
				/*fmt.Printf("[serverPool.NewServerPool]add the delete server %s to CadidateServer %v time %s \n",
				server, p.CandidateServer, time.Now())*/

			}()
		}
	}
}

func (p *ServerPool) resetPoolServer() {
	normal := make([]string, 0, len(p.CandidateServer))
	for k, v := range p.CandidateStatus {
		if v.Online {
			normal = append(normal, k)
		}
	}
	p.CandidateServer = normal
	p.consistentHashMap = map[int]string{}
	p.consistentHashKeys = make([]int, 0, len(normal))
	p.addServer(normal)
	p.errorRate = float64((len(p.CandidateStatus) - len(normal)) / len(p.CandidateStatus))
}
