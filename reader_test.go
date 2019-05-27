package gosar_test

import (
	"testing"

	"github.com/savardiego/gosar"
)

func TestScanCellID(t *testing.T) {
	shape := "cat0Polygon.shp"
	gosar.ScanCellID(shape)
}

func TestScanLoop(t *testing.T) {
	shape := "cat0Polygon.shp"
	gosar.ScanLoop(shape)
}

func TestScanCoverage(t *testing.T) {
	shape := "cat0Polygon.shp"
	gosar.ScanCoverage(shape)
}
