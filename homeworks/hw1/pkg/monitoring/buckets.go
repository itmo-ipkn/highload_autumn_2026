package monitoring

// durationBuckets are the finite histogram bounds in seconds. They are the
// same for every process: nothing in the environment can replace them.
// The Prometheus client adds the +Inf bucket itself.
//
// The low end starts at 500ns because measured handler time for the in-memory
// routes sits around 1–10µs. A first bound of 100µs put essentially every
// observation into one bucket.
var durationBuckets = []float64{
	0.0000005, 0.000001, 0.0000025, 0.000005,
	0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005,
	0.01, 0.025, 0.05,
	0.1, 0.25, 0.5,
	1, 2.5, 5,
}

// DurationBuckets returns a copy of the finite histogram bounds in seconds.
func DurationBuckets() []float64 {
	out := make([]float64, len(durationBuckets))
	copy(out, durationBuckets)
	return out
}
