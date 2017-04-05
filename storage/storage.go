package storage

import (
	"sync"
	"time"

	"github.com/dhconnelly/rtreego"
	"github.com/kdrake/nearestdots/storage/lru"
	"github.com/pkg/errors"
)

type (
	// Location used for storing driver's location
	Location struct {
		Lat float64
		Lon float64
	}
	// Driver model to store driver data
	Driver struct {
		ID           int
		LastLocation Location
		Expiration   int64
		Locations    *lru.LRU
	}
)

// Expired return true if the item has expired
func (d *Driver) Expired() bool {
	if d.Expiration == 0 {
		return false
	}
	return time.Now().UnixNano() > d.Expiration
}

// Bounds method needs for correct working of rtree
// Lat - Y, Lon - X on coordinate system
func (d *Driver) Bounds() *rtreego.Rect {
	return rtreego.Point{d.LastLocation.Lat, d.LastLocation.Lon}.ToRect(0.01)
}

// ErrDriverDoesNotExist sign what driver does not exist
var ErrDriverDoesNotExist = errors.New("Driver does not exist")

// DriverStorage is main storage for our project
type DriverStorage struct {
	mu        *sync.RWMutex
	drivers   map[int]*Driver
	locations *rtreego.Rtree
	lruSize   int
}

// New creates new instance of DriverStorage
func New(lruSize int) *DriverStorage {
	s := new(DriverStorage)
	s.drivers = make(map[int]*Driver)
	s.locations = rtreego.NewTree(2, 25, 50)
	s.mu = new(sync.RWMutex)
	s.lruSize = lruSize
	return s
}

// Set an Driver to the storage, replacing any existing item.
func (s *DriverStorage) Set(driver *Driver) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.drivers[driver.ID]
	if !ok {
		d = driver
		cache, err := lru.New(s.lruSize)
		if err != nil {
			return errors.Wrap(err, "could not create LRU")
		}
		d.Locations = cache
		s.locations.Insert(d)
	}
	d.LastLocation = driver.LastLocation
	d.Locations.Add(time.Now().UnixNano(), d.LastLocation)
	d.Expiration = driver.Expiration

	s.drivers[driver.ID] = driver
	return nil
}

// Delete deletes a driver from storage. Does nothing if the driver is not in the storage.
func (s *DriverStorage) Delete(id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	driver, ok := s.drivers[id]
	if !ok {
		return ErrDriverDoesNotExist
	}
	deleted := s.locations.Delete(driver)
	if deleted {
		delete(s.drivers, driver.ID)
		return nil
	}
	return errors.New("could not remove item")
}

// Get gets driver from storage and an error if nothing found
func (s *DriverStorage) Get(id int) (*Driver, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	driver, ok := s.drivers[id]
	if !ok {
		return nil, ErrDriverDoesNotExist
	}
	return driver, nil
}

// Nearest returns nearest drivers by location
func (s *DriverStorage) Nearest(point rtreego.Point, count int) []*Driver {
	s.mu.Lock()
	defer s.mu.Unlock()

	results := s.locations.NearestNeighbors(count, point)
	var drivers []*Driver
	for _, item := range results {
		if item == nil {
			continue
		}
		drivers = append(drivers, item.(*Driver))
	}
	return drivers
}

// DeleteExpired removes all expired items from storage
func (s *DriverStorage) DeleteExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, d := range s.drivers {
		if d.Expired() {
			deleted := s.locations.Delete(d)
			if deleted {
				delete(s.drivers, d.ID)
			}
		}
	}
}
