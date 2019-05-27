package gosar

import (
	"fmt"
	"log"

	"github.com/golang/geo/s2"
	shp "github.com/savardiego/go-shp"
)

func ScanAndPrint(shape string) {
	cat, err := shp.Open(shape)
	if err != nil {
		log.Fatal(err)
	}
	defer cat.Close()
	fields := cat.Fields()
	for cat.Next() {
		r, s := cat.Shape()
		poly := s.(*shp.Polygon)
		fmt.Printf("Polygon: row:%d points:%d arr:%v\n", r, poly.NumPoints, poly.Points)
		for i, f := range fields {
			v := cat.ReadAttribute(r, i)
			fmt.Printf("%s:%s ", f, v)
		}
		fmt.Printf(". \n")
		// gh := geohash.Encode(lat, lon)

	}
}

func ScanCellID(shape string) {
	cat, err := shp.Open(shape)
	if err != nil {
		log.Fatal(err)
	}
	defer cat.Close()
	for cat.Next() {
		r, s := cat.Shape()
		fmt.Printf("Row: %d\n", r)
		poly := s.(*shp.Polygon)
		for i, p := range poly.Points {
			ll := s2.LatLngFromDegrees(p.Y, p.X)
			cellID := s2.CellIDFromLatLng(ll)
			for t := cellID.Level(); t >= 0; t-- {
				p := cellID.Parent(t)
				min := p.RangeMin()
				max := p.RangeMax()
				inside := cellID <= max && cellID >= min
				fmt.Printf("CellID %s parent of level %d is %s Range(%s   %s) \n", cellID.ToToken(), t, p.ToToken(), p.RangeMin().ToToken(), p.RangeMax().ToToken())
				fmt.Printf("CellID %d parent of level %d is %d Range(%d   %d) included? %t\n", cellID, t, p, p.RangeMin(), p.RangeMax(), inside)
			}
			fmt.Printf("Point %d: %v  CellID Token:%s Level:%v\n", i, ll, cellID, cellID.Level())
		}
	}
}

func ScanLoop(shape string) {
	cat, err := shp.Open(shape)
	if err != nil {
		log.Fatal(err)
	}
	defer cat.Close()
	for cat.Next() {
		r, s := cat.Shape()
		// fmt.Printf("Row: %d\n", r)
		poly := s.(*shp.Polygon)
		cX := (poly.MaxX - poly.MinX) / 2
		cY := (poly.MaxY - poly.MinX) / 2
		cp := s2.PointFromLatLng(s2.LatLngFromDegrees(cY, cX))
		// fmt.Printf("Centre     point: %v\n", cp)
		points := make([]s2.Point, poly.NumPoints)
		for i, v := range poly.Points {
			ll := s2.LatLngFromDegrees(v.Y, v.X)
			p := s2.PointFromLatLng(ll)
			points[i] = p
		}
		loop := s2.LoopFromPoints(points)
		if !loop.ContainsPoint(cp) {
			fmt.Printf("Centre     point: %v\n", cp)
			fmt.Printf("%d Inverting loop: %v    Area:%f\n", r, loop.RectBound(), loop.Area())
			loop.Invert()
			fmt.Printf("%d Inverted  loop: %v     %t     Area:%f\n", r, loop.RectBound(), loop.ContainsPoint(cp), loop.Area())
		}
		// fmt.Printf("Loop          : %v\n", loop)

	}
}

func ScanCoverage(shape string) {
	cat, err := shp.Open(shape)
	if err != nil {
		log.Fatal(err)
	}
	defer cat.Close()
	for cat.Next() {
		r, s := cat.Shape()
		// fmt.Printf("Row: %d\n", r)
		poly := s.(*shp.Polygon)
		cX := (poly.MaxX - poly.MinX) / 2
		cY := (poly.MaxY - poly.MinX) / 2
		cp := s2.PointFromLatLng(s2.LatLngFromDegrees(cY, cX))
		// fmt.Printf("Centre     point: %v\n", cp)
		points := make([]s2.Point, poly.NumPoints)
		for i, v := range poly.Points {
			ll := s2.LatLngFromDegrees(v.Y, v.X)
			p := s2.PointFromLatLng(ll)
			points[i] = p
		}
		loop := s2.LoopFromPoints(points)
		if !loop.ContainsPoint(cp) {
			fmt.Printf("Centre     point: %v\n", cp)
			fmt.Printf("%d Inverting loop: %v    Area:%f\n", r, loop.RectBound(), loop.Area())
			loop.Invert()
			fmt.Printf("%d Inverted  loop: %v     %t     Area:%f\n", r, loop.RectBound(), loop.ContainsPoint(cp), loop.Area())
		}
		// fmt.Printf("Loop          : %v\n", loop)

	}
}
