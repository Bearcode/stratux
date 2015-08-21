// Copyright (c) 2015 Joseph D Poirier
// Distributable under the terms of The New BSD License
// that can be found in the LICENSE file.

// Package dump978 wraps libdump978, a 978MHz UAT demodulator.

package dump978

import (
	"reflect"
	"unsafe"
)

/*
#include <stdint.h>
#include "dump978/dump978.h"
*/
import "C"

// outChan is a buffered output channel for demodulated data.
var OutChan = make(chan []byte, 100)

//export dump978Cb
func dump978Cb(updown C.char, data *C.uint8_t, length C.int) {
	outData := make([]byte, length+1)
	// c buffer to go slice without copying
	var buf []byte
	b := (*reflect.SliceHeader)((unsafe.Pointer(&data)))
	b.Cap = int(length)
	b.Len = int(length)
	b.Data = uintptr(unsafe.Pointer(data))

	// copy incoming to outgoing
	copy(outData, buf[1:])
	outData[0] = byte(updown)
	OutChan <- outData
}