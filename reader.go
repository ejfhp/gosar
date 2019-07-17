package gosar

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/golang/geo/s2"
	shp "github.com/savardiego/go-shp"
)

const timeLayout = "20060102"
const datastoreMAX = 500

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
	counter := 0
	inverted := 0
	cells := make(map[string]int)
	cellids := 0
	coverer := &s2.RegionCoverer{MinLevel: 1, MaxLevel: 15, MaxCells: 8}
	for cat.Next() {
		counter++
		_, s := cat.Shape()
		poly := s.(*shp.Polygon)
		cX := (poly.MaxX + poly.MinX) / 2
		cY := (poly.MaxY + poly.MinY) / 2
		llc := s2.LatLngFromDegrees(cY, cX)
		cp := s2.PointFromLatLng(llc)
		// fmt.Printf("POLY MAXXY MINXY: %f %f    %f %f\n", poly.MaxX, poly.MaxY, poly.MinX, poly.MinY)
		// fmt.Printf("[LON LAT] C[%f, %f] P[%f, %f]\n", llc.Lng.Degrees(), llc.Lat.Degrees(), cX, cY)
		points := make([]s2.Point, poly.NumPoints)
		for i, v := range poly.Points {
			ll := s2.LatLngFromDegrees(v.Y, v.X)
			p := s2.PointFromLatLng(ll)
			points[i] = p
		}
		loop := s2.LoopFromPoints(points)
		if !loop.ContainsPoint(cp) {
			// fmt.Printf("------------  Inverting!\n")
			inverted++
			loop.Invert()
		}
		// fmt.Printf("LOOP (LON, LAT):[")
		// for _, p := range loop.Vertices() {
		// ll := s2.LatLngFromPoint(p)
		// fmt.Printf("[%f, %f], ", ll.Lng.Degrees(), ll.Lat.Degrees())
		// }
		// fmt.Printf("]\n")
		cellUnion := coverer.Covering(loop)
		cellids += len(cellUnion)
		fmt.Printf("Union for loop size:%d  CellID:", len(cellUnion))
		for _, ci := range cellUnion {
			cells[ci.ToToken()]++
			fmt.Printf(" %s ", ci.ToToken())
		}
		fmt.Printf("\n")
	}
	fmt.Printf("Lines:%d Inverted:%d  -  CellID: unique:%d total:%d average:%f\n", counter, inverted, len(cells), cellids, float64(cellids)/float64(counter))
}

func ScanMetadata(shape string) {
	cat, err := shp.Open(shape)
	if err != nil {
		log.Fatal(err)
	}
	defer cat.Close()
	counter := 0
	inverted := 0
	cells := make(map[string]int)
	cellids := 0
	coverer := &s2.RegionCoverer{MinLevel: 1, MaxLevel: 15, MaxCells: 8}

	fields := cat.Fields()
	// for i, f := range fields {
	// 	//removes spaces from the name
	// 	fName := strings.Trim(string(f.Name[:]), "\x00")
	// }

	for cat.Next() {
		counter++
		r, s := cat.Shape()
		poly := s.(*shp.Polygon)
		cX := (poly.MaxX + poly.MinX) / 2
		cY := (poly.MaxY + poly.MinY) / 2
		llc := s2.LatLngFromDegrees(cY, cX)
		cp := s2.PointFromLatLng(llc)
		points := make([]s2.Point, poly.NumPoints)
		for i, v := range poly.Points {
			ll := s2.LatLngFromDegrees(v.Y, v.X)
			p := s2.PointFromLatLng(ll)
			points[i] = p
		}
		loop := s2.LoopFromPoints(points)
		if !loop.ContainsPoint(cp) {
			inverted++
			loop.Invert()
		}
		cellUnion := coverer.Covering(loop)
		cellids += len(cellUnion)
		fmt.Printf("Union for loop size:%d  CellID:", len(cellUnion))
		for _, ci := range cellUnion {
			cells[ci.ToToken()]++
			fmt.Printf(" %s ", ci.ToToken())
		}
		fmt.Printf("\n")
		for c, f := range fields {
			value := cat.ReadAttribute(r, c)
			fmt.Printf("%s:%s\n", f, value)
		}

	}
	fmt.Printf("Lines:%d Inverted:%d  -  CellID: unique:%d total:%d average:%f\n", counter, inverted, len(cells), cellids, float64(cellids)/float64(counter))
}

type Image struct {
	CellID     []string       `datastore:"cellid"`
	WKT        string         `datastore:"wkt"`
	Key        *datastore.Key `datastore:"__key__"`
	Mission    string         `datastore:"mission"`
	Category   int64          `datastore:"category"`
	Quality    string         `datastore:"quality"`
	Imgmod     string         `datastore:"imgmod"`
	Incmin     float64        `datastore:"incmin"`
	Incmax     float64        `datastore:"incmax"`
	Absorbit   int64          `datastore:"absorbit"`
	Relorbit   int64          `datastore:"relorbit"`
	Lookingdir string         `datastore:"lookingdir"`
	Polmod     string         `datastore:"polmod"`
	Polchan    string         `datastore:"polchan"`
	Beamid     string         `datastore:"beamid"`
	Pathdir    string         `datastore:"pathdir"`
	Bandwidth  int64          `datastore:"bandwidth"`
	Antmod     string         `datastore:"antmod"`
	Centertim  time.Time      `datastore:"centertime"`
	Stoptime   time.Time      `datastore:"stoptime"`
	Starttime  time.Time      `datastore:"starttime"`
	Cattime    time.Time      `datastore:"cattime"`
	Resolution string         `datastore:"resolution"`
	Datastack  string         `datastore:"datastack"`
	Resolutio0 float64        `datastore:"resolution0"`
	Resolutio1 float64        `datastore:"resolution1"`
	Browseimag string         `datastore:"browseimag"`
	Starttim0  time.Time      `datastore:"starttim0"`
	Stoptime0  time.Time      `datastore:"stoptime0"`
}

func ToCellIDTokens(cells []s2.CellID) []string {
	cids := make([]string, 0, len(cells))
	for _, ci := range cells {
		cids = append(cids, ci.ToToken())
	}
	return cids
}

type Catalogue struct {
	cat     *shp.Reader
	coverer *s2.RegionCoverer
}

func NewCatalogue(file string) *Catalogue {
	cat, err := shp.Open(file)
	if err != nil {
		log.Fatalf("Cannot open catalogue file %s due to: %v", file, err)
	}
	coverer := &s2.RegionCoverer{MinLevel: 8, MaxLevel: 15, MaxCells: 8}
	catalogue := Catalogue{cat: cat, coverer: coverer}
	return &catalogue
}

func (c *Catalogue) Close() {
	c.cat.Close()
}

func (c *Catalogue) Next() (*Image, bool) {
	ok := c.cat.Next()
	if !ok {
		fmt.Printf("Return false\n")
		return nil, false
	}
	img := Image{}
	row, shape := c.cat.Shape()
	poly := shape.(*shp.Polygon)
	cX := (poly.MaxX + poly.MinX) / 2
	cY := (poly.MaxY + poly.MinY) / 2
	llc := s2.LatLngFromDegrees(cY, cX)
	cp := s2.PointFromLatLng(llc)
	_ = cp
	points := make([]s2.Point, poly.NumPoints)

	wkt := fmt.Sprintf("POLYGON ((")
	for i, v := range poly.Points {
		wkt = wkt + fmt.Sprintf("%f %f,", v.X, v.Y)
		ll := s2.LatLngFromDegrees(v.Y, v.X)
		p := s2.PointFromLatLng(ll)
		points[i] = p
	}
	wkt = wkt + fmt.Sprintf("))")
	img.WKT = wkt
	fmt.Println(wkt)
	//Loops have the internal on the left.
	loop := s2.LoopFromPoints(points)
	area := loop.Area()
	//Currently IsNormalized is much slower, so I use a custom check
	if !loop.ContainsPoint(cp) || area > 6 {
		// if !loop.IsNormalized() {
		// loop.Normalize()
		loop.Invert()
	}
	cellUnion := c.coverer.Covering(loop)
	img.CellID = ToCellIDTokens([]s2.CellID(cellUnion))
	// fmt.Printf("Union for loop size: %d CellIDs: %v\n", len(cellUnion), img.cellIDs)
	//loop to read every single field of a record
	for col, f := range c.cat.Fields() {
		fName := strings.Trim(string(f.Name[:]), "\x00")
		// fmt.Printf(" %s", fName)
		switch fName {
		case "id":
			name := c.cat.ReadAttribute(row, col)
			img.Key = datastore.NameKey("imgtsx", name, nil)
		case "mission":
			img.Mission = c.cat.ReadAttribute(row, col)
		case "category":
			v, err := strconv.ParseInt(c.cat.ReadAttribute(row, col), 10, 64)
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Category = v
		case "quality":
			img.Quality = c.cat.ReadAttribute(row, col)
		case "img_mod":
			img.Imgmod = c.cat.ReadAttribute(row, col)
		case "inc_min":
			v, err := strconv.ParseFloat(c.cat.ReadAttribute(row, col), 64)
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Incmin = v
		case "inc_max":
			v, err := strconv.ParseFloat(c.cat.ReadAttribute(row, col), 64)
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Incmax = v
		case "abs_orbit":
			v, err := strconv.ParseInt(c.cat.ReadAttribute(row, col), 10, 64)
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Absorbit = v
		case "rel_orbit":
			v, err := strconv.ParseInt(c.cat.ReadAttribute(row, col), 10, 64)
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Relorbit = v
		case "looking_dir":
			img.Lookingdir = c.cat.ReadAttribute(row, col)
		case "pol_mod":
			img.Polmod = c.cat.ReadAttribute(row, col)
		case "pol_chan":
			img.Polchan = c.cat.ReadAttribute(row, col)
		case "beam_id":
			img.Beamid = c.cat.ReadAttribute(row, col)
		case "path_dir":
			img.Pathdir = c.cat.ReadAttribute(row, col)
		case "bandwidth":
			v, err := strconv.ParseInt(c.cat.ReadAttribute(row, col), 10, 64)
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Bandwidth = v
		case "ant_mod":
			img.Antmod = c.cat.ReadAttribute(row, col)
		case "center_tim":
			v, err := time.Parse(timeLayout, c.cat.ReadAttribute(row, col))
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Centertim = v
		case "stop_time":
			v, err := time.Parse(timeLayout, c.cat.ReadAttribute(row, col))
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Stoptime = v
		case "start_time":
			v, err := time.Parse(timeLayout, c.cat.ReadAttribute(row, col))
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Starttime = v
		case "cat_time":
			v, err := time.Parse(timeLayout, c.cat.ReadAttribute(row, col))
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Cattime = v
		case "resolution":
			img.Resolution = c.cat.ReadAttribute(row, col)
		case "datastack":
			img.Datastack = c.cat.ReadAttribute(row, col)
		case "resolutio0":
			v, err := strconv.ParseFloat(c.cat.ReadAttribute(row, col), 64)
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Resolutio0 = v
		case "resolutio1":
			v, err := strconv.ParseFloat(c.cat.ReadAttribute(row, col), 64)
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Resolutio1 = v
		case "browseimag":
			img.Browseimag = c.cat.ReadAttribute(row, col)
		case "start_tim0":
			v, err := time.Parse(timeLayout, c.cat.ReadAttribute(row, col))
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Starttim0 = v
		case "stop_time_":
			v, err := time.Parse(timeLayout, c.cat.ReadAttribute(row, col))
			if err != nil {
				log.Printf("ERROR: %v, Field: %s", err, fName)
				break
			}
			img.Stoptime0 = v
		}
	}
	// fmt.Printf("Returning true\n")
	return &img, true
}

//Store received ps from the given channel and store them in burst of DatastoreMax to Google Datastore
func StoreImages(client *datastore.Client, images []*Image) error {
	if len(images) > datastoreMAX {
		return fmt.Errorf("length of images bigger than DatastoreMax: %d", len(images))
	}
	keys := make([]*datastore.Key, 0, len(images))
	var err error

	for _, im := range images {
		keys = append(keys, im.Key)
	}
	if keys, err = client.PutMulti(context.Background(), keys, images); err != nil {
		log.Fatalf("Store Failed! %+v", err)
	}
	fmt.Printf("Image stored: %d\n", len(keys))
	return nil
}

//Store received ps from the given channel and store them in burst of DatastoreMax to Google Datastore
func StoreImage(client *datastore.Client, image *Image) {
	DatastoreKind := "imgtsx"
	var err error
	key := datastore.IncompleteKey(DatastoreKind, nil)
	if key, err = client.Put(context.Background(), key, image); err != nil {
		log.Fatalf("Store Failed! %+v", err)
	}
	fmt.Printf("Key generated: %v", key)
	///just for test
	return
}

func ScanAndStore(shape string) {
	cat := NewCatalogue(shape)
	defer cat.Close()

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, "simcat2-dev")
	_ = client
	if err != nil {
		log.Fatalf("Something went wrong while connecting to the Datastore: %v", err)
	}

	//loop to read every record
	counter := 0
	buffer := make([]*Image, 0, datastoreMAX)
	for im, ok := cat.Next(); ok; im, ok = cat.Next() {
		counter++
		buffer = append(buffer, im)
		if len(buffer) == 500 { //datastoreMAX {
			fmt.Printf("Storing %d. Already stored: %d\n", len(buffer), counter)
			StoreImages(client, buffer)
			buffer = buffer[:0]
		}
	}
	fmt.Printf("Done.\n")
}

func SearchImages(west, south, east, north float64) []*Image {
	points := make([]s2.Point, 4)

	ll := s2.LatLngFromDegrees(south, west)
	points[0] = s2.PointFromLatLng(ll)
	lr := s2.LatLngFromDegrees(south, east)
	points[1] = s2.PointFromLatLng(lr)
	ur := s2.LatLngFromDegrees(north, east)
	points[2] = s2.PointFromLatLng(ur)
	ul := s2.LatLngFromDegrees(north, west)
	points[3] = s2.PointFromLatLng(ul)
	//Loops have the internal on the left.
	loop := s2.LoopFromPoints(points)
	area := loop.Area()
	//Currently IsNormalized is much slower, so I use a custom check
	if area > 6 {
		// if !loop.IsNormalized() {
		// loop.Normalize()
		loop.Invert()
	}
	coverer := &s2.RegionCoverer{MinLevel: 0, MaxLevel: 31, MaxCells: 4}
	cellUnion := coverer.Covering(loop)
	cellRange := make([][]string, len(cellUnion), len(cellUnion))
	fmt.Printf("Cover len: %d\n", len(cellUnion))
	for i, ci := range cellUnion {
		cellRange[i] = make([]string, 2)
		cellRange[i][0] = ci.RangeMin().ToToken()
		cellRange[i][1] = ci.RangeMax().ToToken()
	}
	var results []*Image
	DatastoreKind := "imgtsx"
	for _, r := range cellRange {
		query := datastore.NewQuery(DatastoreKind).Filter("cellid >=", r[0]).Filter("cellid <=", r[1])

		var images []*Image
		ctx := context.Background()
		client, err := datastore.NewClient(ctx, "simcat2-dev")
		if err != nil {
			log.Printf("Query error: %v", err)
		}
		_, err = client.GetAll(ctx, query, &images)
		if err != nil {
			log.Printf("Query error: %v", err)
		}
		results = append(results, images...)
	}
	return results
}

func DeleteAll() {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, "simcat2-dev")
	_ = client
	if err != nil {
		log.Fatalf("Something went wrong while connecting to the Datastore: %v", err)
	}

	query := datastore.NewQuery("imgtsx").KeysOnly()
	data := make([]Image, 0)
	ks, err := client.GetAll(ctx, query, data)
	fmt.Printf("Rows to delete: %d\n", len(ks))
	if err != nil {
		log.Fatalf("Something went wrong while retrieving keys: %v", err)
	}
	for len(ks) > 0 {
		err = client.DeleteMulti(ctx, ks[:int(math.Min(500.0, float64(len(ks))))])
		if err != nil {
			log.Fatalf("Something went wrong while deleting images: %v", err)
		}
		if len(ks) > 500 {
			ks = ks[500:]
		} else {
			ks = ks[:0]
		}
	}
}
