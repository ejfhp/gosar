package gosar_test

import (
	"fmt"
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

func TestScanMetadata(t *testing.T) {
	shape := "cat0Polygon.shp"
	gosar.ScanMetadata(shape)
}

func TestScanAndStore(t *testing.T) {
	shape := "cat0Polygon.shp"
	gosar.ScanAndStore(shape)
}

func TestSearchImages(t *testing.T) {
	north := 46.1
	south := 45.2
	east := 11.5
	west := 10.0
	res := gosar.SearchImages(west, south, east, north)
	for _, im := range res {
		fmt.Printf("image %s cellId: %v WKT: %s\n", im.Key, im.CellID, im.WKT)
	}
	fmt.Printf("Number of images found: %d\n", len(res))
}

func TestDeleteAll(t *testing.T) {
	gosar.DeleteAll()
}
