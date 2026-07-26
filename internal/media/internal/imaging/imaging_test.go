// Package imaging_test builds images in memory and checks what comes back out. No
// fixtures on disk: an encoder writing a gradient is more legible than a binary blob
// nobody can inspect.
package imaging_test

import (
	"bytes"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"testing"

	"comms/internal/media/internal/imaging"
)

// photo returns an encoded JPEG of the given size.
func photo(t *testing.T, width, height int) []byte {
	t.Helper()

	source := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := range height {
		for x := range width {
			source.Set(x, y, color.RGBA{R: uint8(x % 256), G: uint8(y % 256), B: 128, A: 255})
		}
	}

	var encoded bytes.Buffer
	if err := jpeg.Encode(&encoded, source, nil); err != nil {
		t.Fatalf("encode source: %v", err)
	}
	return encoded.Bytes()
}

func measure(t *testing.T, body []byte) (int, int) {
	t.Helper()

	config, _, err := image.DecodeConfig(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("decode result: %v", err)
	}
	return config.Width, config.Height
}

var sizes = []imaging.Rendition{
	{Name: "thumbnail", MaxEdge: 320},
	{Name: "display", MaxEdge: 1280},
}

func TestRenditionsFitTheirBoundAndKeepTheirShape(t *testing.T) {
	t.Parallel()

	results, err := imaging.Derive(photo(t, 2000, 1000), sizes)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d renditions, want 2", len(results))
	}

	for _, result := range results {
		width, height := measure(t, result.Body)
		if width != result.Width || height != result.Height {
			t.Fatalf("%s reports %dx%d and encodes %dx%d",
				result.Name, result.Width, result.Height, width, height)
		}
		// 2:1 in, 2:1 out. A rendition that changes the aspect ratio makes every
		// client's layout wrong.
		if width != 2*height {
			t.Fatalf("%s is %dx%d, which is not 2:1", result.Name, width, height)
		}
	}

	if results[0].Width != 320 || results[1].Width != 1280 {
		t.Fatalf("bounds not applied: %d and %d", results[0].Width, results[1].Width)
	}
}

// TestASmallImageIsNotEnlarged: upscaling spends bytes to look worse.
func TestASmallImageIsNotEnlarged(t *testing.T) {
	t.Parallel()

	results, err := imaging.Derive(photo(t, 100, 80), sizes)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	for _, result := range results {
		if result.Width != 100 || result.Height != 80 {
			t.Fatalf("%s came out %dx%d from a 100x80 source",
				result.Name, result.Width, result.Height)
		}
	}
}

// TestAThumbnailIsSmallerThanItsSource is the point of the whole exercise: a screenful
// of thumbnails must cost less than one original.
func TestAThumbnailIsSmallerThanItsSource(t *testing.T) {
	t.Parallel()

	source := photo(t, 3000, 2000)
	results, err := imaging.Derive(source, sizes)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if len(results[0].Body) >= len(source) {
		t.Fatalf("thumbnail is %d bytes from a %d byte source", len(results[0].Body), len(source))
	}
}

// TestAnExtremeAspectRatioStillEncodes: a 4000x1 image scales to 320x0 under naive
// arithmetic, and a zero-height image cannot be encoded.
func TestAnExtremeAspectRatioStillEncodes(t *testing.T) {
	t.Parallel()

	results, err := imaging.Derive(photo(t, 4000, 1), sizes)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if results[0].Height < 1 {
		t.Fatalf("got height %d", results[0].Height)
	}
}

// TestTransparencyIsFlattenedOntoWhite: JPEG has no alpha, and an unflattened
// transparent PNG comes out with black where it was see-through.
func TestTransparencyIsFlattenedOntoWhite(t *testing.T) {
	t.Parallel()

	source := image.NewRGBA(image.Rect(0, 0, 40, 40))
	// Entirely transparent: every pixel of the result should be white.
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, source); err != nil {
		t.Fatalf("encode source: %v", err)
	}

	results, err := imaging.Derive(encoded.Bytes(), []imaging.Rendition{{Name: "thumbnail", MaxEdge: 320}})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}

	decoded, err := jpeg.Decode(bytes.NewReader(results[0].Body))
	if err != nil {
		t.Fatalf("decode result: %v", err)
	}
	red, green, blue, _ := decoded.At(20, 20).RGBA()
	// JPEG is lossy, so this is "near white" rather than exactly it. Black would be
	// nowhere near.
	if red < 0xF000 || green < 0xF000 || blue < 0xF000 {
		t.Fatalf("transparent pixel came out as %d,%d,%d", red, green, blue)
	}
}

// TestSomethingThatIsNotAnImageFailsPermanently is what stops one bad upload wedging
// the consumer: the caller needs to tell "retry this" from "never retry this".
func TestSomethingThatIsNotAnImageFailsPermanently(t *testing.T) {
	t.Parallel()

	for _, content := range [][]byte{
		[]byte("this is not an image"),
		nil,
		// A truncated JPEG: a plausible header and nothing behind it.
		photo(t, 100, 100)[:20],
	} {
		if _, err := imaging.Derive(content, sizes); !errors.Is(err, imaging.ErrUndecodable) {
			t.Fatalf("got %v, want ErrUndecodable", err)
		}
	}
}

// TestADecompressionBombIsRefusedBeforeItIsDecoded: the size cap does not catch these.
// A few hundred kilobytes of PNG can describe a bitmap larger than the worker's memory,
// so the dimensions are checked from the header before anything is allocated.
func TestADecompressionBombIsRefusedBeforeItIsDecoded(t *testing.T) {
	t.Parallel()

	// A header claiming 20000 x 20000 — four hundred megapixels, which at four bytes a
	// pixel is 1.6 GB if decoded.
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, image.NewGray(image.Rect(0, 0, 20000, 20000))); err != nil {
		t.Fatalf("encode source: %v", err)
	}

	if _, err := imaging.Derive(encoded.Bytes(), sizes); !errors.Is(err, imaging.ErrUndecodable) {
		t.Fatalf("got %v, want ErrUndecodable", err)
	}
}

// TestDerivingTwiceProducesTheSameBytes is what makes reprocessing invisible (MD-2):
// the second pass overwrites the variants with identical content, so a client holding
// the first copy is not looking at something stale.
func TestDerivingTwiceProducesTheSameBytes(t *testing.T) {
	t.Parallel()

	source := photo(t, 1500, 900)
	first, err := imaging.Derive(source, sizes)
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	second, err := imaging.Derive(source, sizes)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}

	for index := range first {
		if !bytes.Equal(first[index].Body, second[index].Body) {
			t.Fatalf("%s differs between passes", first[index].Name)
		}
	}
}
