package columnpkg

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type EncodedColumn struct {
	name              string
	Dv                Seq    //Dictionary Vector
	Av                []byte //Attribute Vector
	AvNr              int    //Nr of Elements in AV
	ElementSize       int    // size of one element (of the AV)
	compressedAv      bool
	compressedAv_type string
}

// Seq für das Sortieren definieren
type Seq []string

func (s Seq) Len() int      { return len(s) }
func (s Seq) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

//Nicht unterscheiden zwischen klein und groß beim Sortieren
func (s Seq) Less(i, j int) bool {
	return strings.ToLower(s[i]) < strings.ToLower(s[j])
}

//helper Function for PrintStats()
func printCol(column []byte, name string) {
	lc := len(column)
	str := "Column " + name + " (Length=" + strconv.Itoa(lc) + ") : "

	if lc < 10 {
		fmt.Println(str, column[0:lc])
	} else {
		fmt.Println(str, column[0:4], "...", column[lc-4:lc])
	}
}

//helper Function for PrintStats()
//find out how to overload a function and combine with printCol()
func printColStr(column []string, name string) {
	lc := len(column)
	str := "Column " + name + " (Length=" + strconv.Itoa(lc) + ") : "

	if lc < 10 {
		fmt.Println(str, column[0:lc])
	} else {
		fmt.Println(str, column[0:4], "...", column[lc-4:lc])
	}
}

func (c *EncodedColumn) PrintStats() {
	fmt.Println("Attribute Vector element length in bit: ", c.ElementSize)
	fmt.Println("Attribute Vector in Bytes: ", len(c.Av))
	fmt.Println("Dictionary Vector (Distinct Records): ", len(c.Dv))
	fmt.Println("Dictionary Vector (size in Byte): ", c.calcDvSize())

	printCol(c.Av, "Attribute Vector")
	printColStr(c.Dv, "Dictionary Vector")
}

//size in Byte
func (c *EncodedColumn) calcDvSize() int {
	ret := 0
	for _, val := range c.Dv {
		ret += len(val) // assumes that one char is one Byte
	}
	return ret
}

func (c *EncodedColumn) DictEnc(column []string) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	c.AvNr = len(column)
	c.Dv = removeDuplicates(column)
	c.ElementSize = getNeededBits(c.Dv)
	sort.Sort(Seq(c.Dv))
	if c.AvNr > 1000 {  	//decides if to run the decoding in parallel
				//if more than 1000 elements in Attribute Vector
		c.buildAVector_parallel(column, runtime.NumCPU()) //parallel on all cores
	} else {
		c.buildAVector(column) //sequential
	}
}

func (c *EncodedColumn) buildAVector(column []string) {
	AV_size := sizeOfAV(c.ElementSize, len(column))
	var AV = make([]byte, AV_size)
	c.Av = AV
	for i, v := range column {
		//Search for v: Position in DictVector -> Write DV Position (payload) in AV
		pos_DV := sort.Search(len(c.Dv),
			func(i int) bool {
				return strings.ToLower(c.Dv[i]) >= strings.ToLower(v)
			})
		//writeElementOfAV(pos_DV, i, oneElementSize, AV)
		c.writeAvElement_new(pos_DV, i)
	}
}

func (c *EncodedColumn) buildAVector_parallel(column []string, workerNr int) {
	AV_size := sizeOfAV(c.ElementSize, len(column))
	var AV = make([]byte, AV_size)
	c.Av = AV
	partSize := c.AvNr / workerNr
	var wg sync.WaitGroup
	for i := 0; i < workerNr; i++ {
		wg.Add(1)
		endsize := (i+1)*partSize
		if i == workerNr - 1 {
			endsize = c.AvNr
		}
		go c.buildAVector_worker(column[i*partSize:endsize], i*partSize, &wg)
	}
	wg.Wait()
	//fmt.Println("All workers finished")
}

func (c *EncodedColumn) buildAVector_worker(column []string, startOffset int, wg *sync.WaitGroup) {
	for k, v := range column {
		//fmt.Print("Column: ", k, " ", v)
		//Search for v: Position in DictVector -> Write DV Position (payload) in AV
		pos_DV := sort.Search(len(c.Dv),
			func(i int) bool {
				return strings.ToLower(c.Dv[i]) >= strings.ToLower(v)
			})
		//fmt.Print("posDV: ", pos_DV, " posAV: ", k+startOffset)
		c.writeAvElement_new(pos_DV, k+startOffset)
	}
	wg.Done()
}

//Schreibe einen Payload (eine DV Position) in den Attribut Vektor (AV)
func (c *EncodedColumn) writeAvElement_new(payload int, posAV int) {
	//1. Aufteilen des Payload in den Teil für dieses Byte und den Rest
	//1.a) Wieviel passt in dieses Byte noch rein? (in bit) Dazu: Beginn (Position) im Byte
	SHIFT := shiftNumber(posAV, c.ElementSize)
	BYTENR := findWriteByte(posAV, c.ElementSize)
	payload_new_size := 8 - SHIFT
	var payload_new byte = byte(payload)
	payload_new = payload_new << uint(SHIFT)
	var payload_rest int = payload >> uint(payload_new_size)
	var payload_rest_size int = c.ElementSize - payload_new_size
	//2. Schreiben was in dieses Byte passt
	c.Av[BYTENR] = c.Av[BYTENR] | payload_new
	//3. Den Rest schreiben
	if payload_rest_size > 0 {
		c.writeAvElement_rest(payload_rest, payload_rest_size, BYTENR+1)
	}
}

//Schreibe restlichen Payload in folgende Bytes
func (c *EncodedColumn) writeAvElement_rest(payload int, payload_size int, byteNR int) {
	c.Av[byteNR] = c.Av[byteNR] | byte(payload)
	if payload_size > 8 {
		payload_rest := payload >> 8
		payload_rest_size := payload_size - 8
		c.writeAvElement_rest(payload_rest, payload_rest_size, byteNR+1)
	}
}

//What shift is needed for a given Position
func shiftNumber(pos int, size int) int {
	return (pos * size) % 8
}

//What byte to Write to?
func findWriteByte(pos int, size int) int {
	return (pos * size) / 8
}

//überprüfen!!
//if return == 0: fits in Byte, else return == number of bits that don't fit
func fitsInByte(pos int, size int) int {
	return size - shiftNumber(pos, size)
}

//How many Byte are needed for the Attribute Vector?
func sizeOfAV(elementSize, numberOfElements int) int {
	//fmt.Print("Elementgröße: ", sizeForElementInBit, " ")
	//fmt.Println("Anzahl der Elemente: ", numberOfElements)
	return int(math.Ceil(float64(elementSize*numberOfElements) / 8))
}

//doppelte Einträge entfernen und sortieren
func removeDuplicates(a []string) []string {
	result := []string{}
	seen := map[string]string{}
	for _, val := range a {
		if _, ok := seen[val]; !ok {
			result = append(result, val)
			seen[val] = val
		}
	}
	return result
}

//How many Bit are needed to count all Elements (Cardinality of Column)
func getNeededBits(column []string) int {
	length := math.Floor(math.Log2(float64(len(column))))

	return int(length) + 1
}

//DECODE
func (c *EncodedColumn) DecodeCol() []string {
	//size_ele := getNeededBits(c.Dv)
	var column = make([]string, c.AvNr)
	value := new(int)
	// Schleife für jedes Element des AV (nicht Byte des AV)
	for i := 0; i < c.AvNr; i++ {
		*value = 0
		var posAV int = c.ElementSize * i
		c.decodeValPart_new(value, posAV) //posAV ist die bit position des Elements
		column[i] = c.Dv[*value]
	}
	return column
}

func (c *EncodedColumn) decodeValPart_new(value *int, posAV int) {
	SHIFT := posAV % 8
	BYTENR := posAV / 8
	payload_size := c.ElementSize
	var payload_size_new int = 0
	//fmt.Println("Bytenr: ", BYTENR, " Länge: ", len(c.Av))
	BYTE := c.Av[BYTENR]

	if payload_size+SHIFT < 8 {
		payload_size_new = payload_size
		left_shift := 8 - (payload_size + SHIFT)
		BYTE = BYTE << uint(left_shift)
	} else {
		payload_size_new = 8 - SHIFT
	}

	BYTE = BYTE >> uint(8-payload_size_new)
	*value = *value | int(BYTE)
	offset := payload_size_new
	rest_size := payload_size - payload_size_new

	if rest_size > 0 {
		c.decodeValPart_rest(value, BYTENR+1, rest_size, offset)
	}
}

func (c *EncodedColumn) decodeValPart_rest(value *int, BYTENR int,
	payload_size int, offset int) {
	BYTE := c.Av[BYTENR]
	var payload_size_new int = 0
	if payload_size < 8 {
		payload_size_new = payload_size
		left_shift := 8 - payload_size
		BYTE = BYTE << uint(left_shift)
	} else {
		payload_size_new = 8
	}
	var INTEGER int = int(BYTE >> uint(8-payload_size_new))
	//right shift um offset
	INTEGER = INTEGER << uint(offset)
	*value = *value | INTEGER
	rest_size := payload_size - payload_size_new
	if rest_size > 0 {
		c.decodeValPart_rest(value, BYTENR+1, rest_size, offset+payload_size_new)
	}
}

func (c *EncodedColumn) findAvPositionsForValue(searchStr string) []int {
	var result []int

	DvPos := c.findDvPosForValue(searchStr)
	value := new(int)
	var posAV int
	for i := 0; i < c.AvNr; i++ {
		*value = 0
		posAV = c.ElementSize * i
		c.decodeValPart_new(value, posAV) //posAV ist die bit position des Elements
		if DvPos == *value {
			result = append(result, i)
		}
	}
	return result
}

func (c *EncodedColumn) findDvPosForValue(searchStr string) int {
	var result int = -1
	i := sort.Search(len(c.Dv), func(i int) bool { return c.Dv[i] >= searchStr })
	if i < len(c.Dv) && c.Dv[i] == searchStr {
		result = i
	}
	return result
}
