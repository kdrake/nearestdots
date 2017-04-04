package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStorage(t *testing.T) {
	s := New()
	driver := &Driver{
		ID: 1,
		LastLocation: Location{
			Lat: 1,
			Lon: 1,
		},
	}
	s.Set(driver.ID, driver)

	d, err := s.Get(driver.ID)
	assert.NoError(t, err)
	assert.Equal(t, d, driver)

	err = s.Delete(driver.ID)
	assert.NoError(t, err)

	d, err = s.Get(driver.ID)
	assert.Equal(t, err, ErrDriverDoesNotExist)
}

func TestNearest(t *testing.T) {
	s := New()
	s.Set(123, &Driver{
		ID: 123,
		LastLocation: Location{
			Lat: 1,
			Lon: 1,
		},
	})
	s.Set(666, &Driver{
		ID: 666,
		LastLocation: Location{
			Lat: 42.875799,
			Lon: 74.588279,
		},
	})
	drivers, _ := s.Nearest(1000, 42.876420, 74.588332)
	assert.Equal(t, 1, len(drivers))
}

func BenchmarkNearest(b *testing.B) {
	s := New()
	for i := 0; i < 100; i++ {
		s.Set(i, &Driver{
			ID: i,
			LastLocation: Location{
				Lat: float64(i),
				Lon: float64(i),
			},
		})
	}
	for i := 0; i < b.N; i++ {
		s.Nearest(1000, 123, 123)
	}
}
