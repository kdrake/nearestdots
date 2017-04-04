package storage

import (
	"errors"
	"math"

	"github.com/dhconnelly/rtreego"
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
	}
)

// Bounds method needs for correct working of rtree
// Lat - Y, Lon - X on coordinate system
func (d *Driver) Bounds() *rtreego.Rect {
	return rtreego.Point{d.LastLocation.Lat, d.LastLocation.Lon}.ToRect(0.01)
}

// ErrDriverDoesNotExist sign what driver does not exist
var ErrDriverDoesNotExist = errors.New("Driver does not exist")

// DriverStorage is main storage for our project
type DriverStorage struct {
	drivers   map[int]*Driver
	locations *rtreego.Rtree
}

// New creates new instance of DriverStorage
func New() *DriverStorage {
	d := &DriverStorage{}
	d.drivers = make(map[int]*Driver)
	d.locations = rtreego.NewTree(2, 25, 50)
	return d
}

// Set sets driver to storage by key
func (d *DriverStorage) Set(key int, driver *Driver) {
	_, ok := d.drivers[key]
	if !ok {
		d.locations.Insert(driver)
	}
	d.drivers[key] = driver
}

// Delete removes driver from storage by key
func (d *DriverStorage) Delete(key int) error {
	driver, ok := d.drivers[key]
	if !ok {
		return ErrDriverDoesNotExist
	}
	if d.locations.Delete(driver) {
		delete(d.drivers, key)
		return nil
	}
	return errors.New("could not remove driver")
}

// Get gets driver from storage and an error if nothing found
func (d *DriverStorage) Get(key int) (*Driver, error) {
	driver, ok := d.drivers[key]
	if !ok {
		return nil, ErrDriverDoesNotExist
	}
	return driver, nil
}

// Nearest returns nearest drivers by location
func (d *DriverStorage) Nearest(count int, lat, lon float64) []*Driver {
	point := rtreego.Point{lat, lon}
	results := d.locations.NearestNeighbors(count, point)
	var drivers []*Driver
	for _, item := range results {
		if item == nil {
			continue
		}
		drivers = append(drivers, item.(*Driver))
	}
	return drivers
}

// haversin(θ) function
func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}

// Distance function returns the distance (in meters) between two points of
//     a given longitude and latitude relatively accurately (using a spherical
//     approximation of the Earth) through the Haversin Distance Formula for
//     great arc distance on a sphere with accuracy for small distances
//
// point coordinates are supplied in degrees and converted into rad. in the func
//
// distance returned is METERS!!!!!!
// http://en.wikipedia.org/wiki/Haversine_formula
func Distance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians
	// must cast radius as float to multiply later
	var la1, lo1, la2, lo2, r float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180
	r = 6378100 // Earth radius in METERS
	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)
	return 2 * r * math.Asin(math.Sqrt(h))
}
