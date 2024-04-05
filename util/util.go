package util

// ChunkSlice is a generic function that takes a slice of any type and an integer as input.
// It divides the input slice into packJobs of size 'chunkSize' and returns a 2D slice.
// If 'chunkSize' is less than or equal to zero, it returns an empty 2D slice.
//
// Parameters:
//
//   - slice: A slice of any type that needs to be chunked.
//   - chunkSize: The size of each packJob. Must be a positive integer.
//
// Returns:
//
//   - A 2D slice where each inner slice is of length 'chunkSize'.
//     The last inner slice may be shorter if the length of 'slice' is not a multiple of 'chunkSize'.
func ChunkSlice[T any](slice []T, chunkSize int) [][]T {
	var packJobs [][]T
	if chunkSize <= 0 {
		return packJobs
	}

	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		packJobs = append(packJobs, slice[i:end])
	}

	return packJobs
}

// NextPowerOfTwo calculates the smallest power of two that is greater than or equal to x.
// If x is already a power of two, it returns x. For x equal to 0, the result is 1.
//
// Parameters:
//   - x: The input value for which the next power of two needs to be calculated.
//
// Returns:
// The smallest power of two that is greater than or equal to x.
func NextPowerOfTwo(x uint64) uint64 {
	if x == 0 {
		return 1
	}

	// Find the position of the highest bit set to 1
	pos := uint(0)
	for shifted := x; shifted > 0; shifted >>= 1 {
		pos++
	}

	// If x is already a power of two, return x
	if x == 1<<(pos-1) {
		return x
	}

	// Otherwise, return the next power of two
	return 1 << pos
}
