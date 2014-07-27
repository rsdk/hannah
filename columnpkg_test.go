package columnpkg

import (
	"testing"
)

var alpha = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K",
	"L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}
var col_one = []string{"Eins"}
var col_small = []string{"Omega", "zulu", "aaa", "Kathode", "zwei", "zulu",
	"aaa", "drei", "anhalter", "zulu", "Berg", "zulu", "zulu", "zulu",
	"zulu", "Mauer", "zwei", "drei", "drei"}

func prep(alpha []string, size int) []string {
	var ret = make([]string, size)
	for i := 0; i < size; i++ {
		ret[i] = alpha[i%26] + alpha[(i+20)%6] + alpha[(i+10)%20]
	}
	return ret
}

func Test_DictEncodingDecoding_1(t *testing.T) {

	//col_one
	mycolumn1 := new(EncodedColumn)
	mycolumn1.DictEnc(col_one)
	resultcol1 := mycolumn1.DecodeCol()

	for i := 0; i < len(col_one); i++ {
		if col_one[i] != resultcol1[i] {
			t.Error("Fehler beim Arrayvergleich. Position: ", i, " Soll: ", col_one[i], " Ist: ", resultcol1[i])
		}
	}
	if len(col_one) != len(resultcol1) {
		t.Error("DictEncodingDecoding: Länge des Ergebnisarrays entspricht nicht der Länge des Ursprungsarrays. ")
	}

	//col_small
	mycolumn2 := new(EncodedColumn)
	mycolumn2.DictEnc(col_small)
	resultcol2 := mycolumn2.DecodeCol()

	for i := 0; i < len(col_small); i++ {
		if col_small[i] != resultcol2[i] {
			t.Error("Fehler beim Arrayvergleich. Position: ", i, " Soll: ", col_small[i], " Ist: ", resultcol2[i])
		}
	}
	if len(col_small) != len(resultcol2) {
		t.Error("DictEncodingDecoding: Länge des Ergebnisarrays entspricht nicht der Länge des Ursprungsarrays. ")
	}

	//col gößer als 256 Einträge
	col3 := prep(alpha, 300)

	mycolumn3 := new(EncodedColumn)
	mycolumn3.DictEnc(col3)
	resultcol3 := mycolumn3.DecodeCol()

	for i := 0; i < len(col3); i++ {
		if col3[i] != resultcol3[i] {
			t.Error("Fehler beim Arrayvergleich. Position: ", i, " Soll: ", col3[i], " Ist: ", resultcol3[i])
		}
	}
	if len(col3) != len(resultcol3) {
		t.Error("DictEncodingDecoding: Länge des Ergebnisarrays entspricht nicht der Länge des Ursprungsarrays. ")
	}

}

func Test_getNeededBits_1(t *testing.T) {

	if bits := getNeededBits(col_small); bits != 5 {
		t.Error("Die Anzahl der nötigen Bits für die Darstellung des Dictionary Vectors. Soll: 5 Ist: ", bits)
	}
}

func Test_findDvPosForValue_1(t *testing.T) {

	mycolumn := new(EncodedColumn)
	mycolumn.DictEnc(col_small)

	if result := mycolumn.findDvPosForValue("zulu"); result != 7 {
		t.Error("Erste Position eines Strings im Dictionary Vector finden. Soll: 7 Ist: ", result)
	}

	if result := mycolumn.findDvPosForValue("Hallo"); result != -1 {
		t.Error("Erste Position eines Strings im Dictionary Vector finden. Soll: -1 (nicht vorhanden) Ist: ", result)
	}
}

func Test_findAvPositionsForValue(t *testing.T) {
	mycolumn := new(EncodedColumn)
	mycolumn.DictEnc(col_small)

	result := mycolumn.findAvPositionsForValue("zulu")
	if len(result) != 7 {
		t.Error("Anzahl eines Wertes im AV. Soll: 7 Ist: ", result)
	}

	compare := []int{1, 5, 9, 11, 12, 13, 14}

	for i := 0; i < len(result); i++ {
		if result[i] != compare[i] {
			t.Error("Gefundener Wert im AV. Soll: ,", compare[i], " Ist: ", result[i])
		}
	}
}

func Test_getNeededBits(t *testing.T) {
	number1 := getNeededBits(col_small)
	if number1 != 5 {
		t.Error("Benötigte Bit. Soll: 5 Ist: ", number1)
	}

	number2 := getNeededBits(col_one)
	if number2 != 1 {
		t.Error("Benötigte Bit. Soll: 1 Ist: ", number2)
	}
}

//********************************************
// Benchmarks
//********************************************
func BenchmarkEncode(b *testing.B) {
	col := prep(alpha, 10000000)
	mycolumn := new(EncodedColumn)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mycolumn.DictEnc(col)
	}
}

func BenchmarkDecode(b *testing.B) {
	col := prep(alpha, 10000000)
	mycolumn := new(EncodedColumn)
	mycolumn.DictEnc(col)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mycolumn.DecodeCol()

	}
}

func BenchmarkFindAVPos(b *testing.B) {
	col := prep(alpha, 10000000)
	col[4000] = "test"
	col[4500] = "test"
	col[80000] = "test"
	mycolumn := new(EncodedColumn)
	mycolumn.DictEnc(col)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mycolumn.findAvPositionsForValue("test")
	}
}
