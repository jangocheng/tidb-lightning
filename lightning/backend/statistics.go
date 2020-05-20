package backend

import (
	"github.com/pingcap/tidb/util/fastrand"
	"sort"
)

type SampleBuilder struct {
	samples       []uint64
	count         uint64
	maxSampleSize uint64
}

func NewSampleBuilder(maxSize uint64) *SampleBuilder {
	return &SampleBuilder{
		samples:       make([]uint64, 0, int(maxSize)),
		count:         0,
		maxSampleSize: maxSize,
	}
}

func (b *SampleBuilder) Add(val uint64) {
	b.count++
	if len(b.samples) < int(b.maxSampleSize) {
		b.samples = append(b.samples, val)
	} else {
		r := fastrand.Uint64N(b.count)
		if r < b.maxSampleSize {
			b.samples[int(r)] = val
		}
	}
}

func (b *SampleBuilder) Merge(s *SampleBuilder) {

	step := int(s.count) / len(s.samples)
	for i, v := range s.samples {
		if len(b.samples) < int(b.maxSampleSize) {
			b.samples = append(b.samples, v)
		} else {
			r := fastrand.Uint64N(b.count + uint64(i * step))
			if r <= s.count {
				idx := r % b.maxSampleSize
				b.samples[int(idx)] = v
			}
		}
	}
	b.count += s.count
}

type Bucket struct {
	count int
	repeat int
	lower uint64
	upper uint64
}

type Histogram struct {
	buckets []Bucket
}

func NewHistogram(bucketSize int) *Histogram {
	return &Histogram{
		buckets: make([]Bucket, 0, bucketSize),
	}
}

func (b *SampleBuilder) Build(bucketSize int) *Histogram {
	if b.count == 0 || len(b.samples) == 0 {
		return NewHistogram(0)
	}

	samples := b.samples
	sort.Slice(samples, func(i, j int) bool {
		return samples[i] < samples[j]
	})

	buckets := make([]Bucket, 0, bucketSize)

	sampleNum := int64(len(samples))
	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(b.count) / float64(sampleNum)
	valuesPerBucket := float64(b.count) / float64(bucketSize) + sampleFactor

	bucketIdx := 0
	buckets = append(buckets, Bucket{
		count:int(sampleFactor),
		repeat: int(sampleFactor),
		lower:samples[0],
		upper: samples[0],
	})

	var lastCount int
	for i := 1; i < int(sampleNum); i++ {
		totalCount := float64(i+1) * sampleFactor
		if buckets[bucketIdx].upper == samples[i] {
			buckets[bucketIdx].count = int(totalCount)
			buckets[bucketIdx].repeat += int(sampleFactor)
		} else if totalCount - float64(lastCount) <= valuesPerBucket {
			buckets[bucketIdx].upper = samples[i]
			buckets[bucketIdx].count = int(totalCount)
			buckets[bucketIdx].repeat = int(sampleFactor)
		} else {
			lastCount = buckets[bucketIdx].count
			buckets = append(buckets, Bucket{
				count:int(totalCount),
				repeat: int(sampleFactor),
				lower:samples[i],
				upper: samples[i],
			})
			bucketIdx++
		}
	}
	return &Histogram{buckets:buckets}
}