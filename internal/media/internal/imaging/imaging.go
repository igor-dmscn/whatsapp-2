// Package imaging derives smaller renditions of a photo.
//
// The only part of this system that reads the content of what a user sent (ADR-0001),
// and it is deliberately a leaf: bytes in, bytes out, no database, no network, no
// knowledge of attachments. That is what makes it testable against images built in
// memory rather than against a running store.
package imaging

import (
	"bytes"
	"errors"
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"time"

	// Registered for their decoders. GIF gives the first frame, which is what a
	// thumbnail of an animation should be; the animation itself is in the retained
	// original.
	_ "image/gif"
	_ "image/png"

	// WebP is decode-only in x/image, which is all that is needed: renditions are
	// written as JPEG.
	_ "golang.org/x/image/webp"

	xdraw "golang.org/x/image/draw"
)

// ErrUndecodable means the bytes are not an image this system can read.
//
// A permanent failure, distinct from a transient one: retrying it forever would wedge
// the consumer on one bad upload, which is exactly the poison pill the projections
// learned about in phase 3.
var ErrUndecodable = errors.New("content could not be decoded as an image")

// quality is the JPEG quality renditions are written at.
//
// 82 is the usual point where further increase costs bytes without a visible
// difference at these sizes. Not configurable: a knob nobody turns is a knob that
// drifts out of agreement with the thumbnails already in the store.
const quality = 82

// decodeBudget bounds how long one image may take to decode and scale.
//
// A decompression bomb is a small file that decodes to an enormous bitmap, and the
// size cap does not catch it — a few hundred kilobytes can describe a hundred
// megapixels. This is the backstop that keeps one upload from occupying a worker.
const decodeBudget = 30 * time.Second

// maxPixels is the other half of that: an image larger than this is refused rather
// than scaled, because the allocation happens during decoding, before any of this
// code gets to look at the dimensions.
//
// 8000 x 8000 covers any phone or camera. It is a limit on the *decoded* bitmap, so
// the memory it bounds is roughly four bytes per pixel.
const maxPixels = 64_000_000

// Rendition is one size to derive.
type Rendition struct {
	Name string
	// MaxEdge bounds the longer side. The aspect ratio is preserved, and an image
	// already smaller than this is left at its own size rather than upscaled — an
	// enlarged thumbnail is bytes spent to look worse.
	MaxEdge int
}

// Result is what one rendition came out as.
type Result struct {
	Name          string
	ContentType   string
	Body          []byte
	Width, Height int
}

// Derive produces every rendition from one source image.
//
// Decoded once for all of them, which is the whole reason this takes a list: decoding
// is the expensive half, and a thumbnail and a display copy of the same photo should
// not pay for it twice.
func Derive(source []byte, renditions []Rendition) ([]Result, error) {
	deadline := time.Now().Add(decodeBudget)

	config, _, err := image.DecodeConfig(bytes.NewReader(source))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUndecodable, err)
	}
	if config.Width*config.Height > maxPixels {
		return nil, fmt.Errorf("%w: %d by %d exceeds the pixel limit",
			ErrUndecodable, config.Width, config.Height)
	}

	decoded, _, err := image.Decode(bytes.NewReader(source))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUndecodable, err)
	}

	results := make([]Result, 0, len(renditions))
	for _, rendition := range renditions {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("%w: exceeded the decoding budget", ErrUndecodable)
		}

		scaled := scale(decoded, rendition.MaxEdge)
		var encoded bytes.Buffer
		if err := jpeg.Encode(&encoded, scaled, &jpeg.Options{Quality: quality}); err != nil {
			return nil, fmt.Errorf("encode %s: %w", rendition.Name, err)
		}

		bounds := scaled.Bounds()
		results = append(results, Result{
			Name:        rendition.Name,
			ContentType: "image/jpeg",
			Body:        encoded.Bytes(),
			Width:       bounds.Dx(),
			Height:      bounds.Dy(),
		})
	}
	return results, nil
}

// scale returns the image fitted within maxEdge, flattened onto white.
//
// Flattened because renditions are JPEG, which has no alpha channel: a transparent
// PNG left to the encoder comes out with black where it was see-through. White is
// what a viewer expects and what every chat client does.
func scale(source image.Image, maxEdge int) image.Image {
	bounds := source.Bounds()
	width, height := bounds.Dx(), bounds.Dy()

	if longest := max(width, height); longest > maxEdge {
		// Rounded rather than truncated, and floored at one pixel: a very wide, very
		// short image would otherwise scale to zero height and fail to encode.
		width = max(1, (width*maxEdge+longest/2)/longest)
		height = max(1, (height*maxEdge+longest/2)/longest)
	}

	target := image.NewRGBA(image.Rect(0, 0, width, height))
	draw.Draw(target, target.Bounds(), image.White, image.Point{}, draw.Src)

	// CatmullRom, because downscaling with a nearest-neighbour kernel produces the
	// aliasing that makes a thumbnail look like a mistake. It is the slowest of the
	// available kernels and this runs in the worker, off the request path.
	xdraw.CatmullRom.Scale(target, target.Bounds(), source, bounds, xdraw.Over, nil)
	return target
}
