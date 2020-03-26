package main

import (
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"log"
	"math"
	"os"
	"sync"
	"time"
)

const L = 244
const n = 48
const totalDays = 1255
const nIntervals = (totalDays - L) * n

var limitK = 2.5 * math.Pow(n, -0.49)

func main() {

	dfIndex := ReadFileIntoDataFrame("SH/SH000906.csv")
	rsIndex := GetRs(dfIndex)
	ksIndex := GetKs(rsIndex)
	indexDates := dfIndex.Col("Date").Records()
	//_, _ = ksIndex, indexDates

	//code := "SH000999"

	//fmt.Println(ContinuousBeta(rs[:L*n], rsIndex[:L*n], ks[:L*n], ksIndex[:L*n]))

	codesDf := ReadFileIntoDataFrame("codes.csv")
	codes := codesDf.Col("code").Records()

	wg := sync.WaitGroup{}
	step := 2
	for i := 0; i < 100; i += step {
		wg.Add(step)
		for j := i; j < i+step; j++ {
			go func(j int) {
				fmt.Println(j, "-th go routine is computing for", codes[j])
				ContinuousBetasForCode(codes[j], indexDates, rsIndex, ksIndex)
				wg.Done()
			}(j)
		}
		wg.Wait()
	}
}

func ContinuousBetasForCode(path string, indexDates []string, rsIndex []float64, ksIndex []float64) {
	df := ReadFileIntoDataFrame(path + ".csv")
	if df.Nrow() <= L*n {
		log.Println(path, "Data length is less than one year")
		return
	}
	rs := GetRs(df)
	ks := GetKs(rs)

	dates := df.Col("Date").Records()
	sameIndex := 0
	for i, indexDate := range indexDates {
		if indexDate == dates[0] {
			sameIndex = i
			break
		}
	}
	dates = dates[L*n:]
	closePrices := df.Col("Close").Float()[L*n:]

	var newDates []string
	var newClosePrices []float64
	for i := 0; i < len(dates)/n; i++ {
		newDates = append(newDates, dates[i*n][:10])
		newClosePrices = append(newClosePrices, closePrices[(i+1)*n-1])
	}
	fmt.Println("Same beginning index", sameIndex)
	betas := ContinuousBetas(rs, rsIndex, ks, ksIndex, sameIndex)
	//fmt.Println(len(newDates), len(betas))
	newDf := dataframe.New(series.New(newDates, series.String, "Date"),
		series.New(newClosePrices, series.Float, "Close"),
		series.New(betas, series.Float, "beta"))
	//fmt.Println(newDf)

	output, err := os.OpenFile("beta/"+path+".csv", os.O_CREATE, 0644)
	if err != nil {
		log.Println(err)
	}
	newDf.WriteCSV(output)
}

func ContinuousBetas(rs []float64, rsIndex []float64, ks []float64, ksIndex []float64, sameIndex int) []float64 {
	rsIndex, ksIndex = rsIndex[sameIndex:], ksIndex[sameIndex:]
	validDays := len(rs)/n - L
	fmt.Println("validDays:", validDays)
	var betas = make([]float64, validDays)

	t := time.Now()

	wg := sync.WaitGroup{}
	wg.Add(validDays)

	for s := 0; s < validDays; s++ {
		go func(s int) {
			start := s * n
			rsEnd := (s + L) * n
			ksEnd := (s + 1) * n
			// be careful when using ks since len(ks) == len(rs) - n*L
			beta := ContinuousBeta(rs[start:rsEnd], rsIndex[start:rsEnd], ks[start:ksEnd], ksIndex[start:ksEnd])
			betas[s] = beta
			//fmt.Printf("Day %d beta = %f\n", s, beta)
			wg.Done()
		}(s)
	}
	wg.Wait()
	fmt.Println("Compute Beta Time consumed:", time.Now().Sub(t).Seconds())
	return betas
}

// Calculate Continuous Beta for one day
// eg. t=245
func ContinuousBeta(rs []float64, rsIndex []float64, ks []float64, ksIndex []float64) float64 {
	var dividend, divisor float64
	for i := range rs {
		r, rIndex := rs[i], rsIndex[i]
		k, kIndex := ks[i%n], ksIndex[i%n]
		//fmt.Println(r, rIndex, k, kIndex)
		if math.Abs(r+rIndex) <= k*kIndex {
			dividend += math.Pow(r+rIndex, 2)
		}
		if math.Abs(r-rIndex) <= k/kIndex {
			dividend -= math.Pow(r-rIndex, 2)
		}
		if math.Abs(rIndex) <= kIndex {
			divisor += math.Pow(rIndex, 2)
		}
	}
	beta := dividend / (4 * divisor)
	return beta
}

func ReadFileIntoDataFrame(filename string) dataframe.DataFrame {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	df := dataframe.ReadCSV(f)
	return df
}

// Ks is an array of k
// length = len(rs) - n * L
func GetKs(rs []float64) []float64 {
	intervals := len(rs) - L*n
	days := len(rs) / n
	var TODs = make([]float64, intervals)
	var BVs = make([]float64, days)
	var RVs = make([]float64, days)
	t := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(days)
	for d := 0; d < days; d++ {
		go func(d int) {
			interval := rs[d*n : (d+1)*n]
			BVs[d] = BV(interval)
			RVs[d] = RV(interval)
			wg.Done()
		}(d)
	}
	wg.Wait()
	wg.Add(intervals)
	for s := 0; s < days-L; s++ {
		for tau := 0; tau < n; tau++ {
			go func(s, tau int) {
				TODs[s*n+tau] = TodAtTau(rs[s:s+L*n], tau, n, BVs[s:], RVs[s:])
				wg.Done()
			}(s, tau)
		}
	}
	wg.Wait()

	wg.Add(intervals)
	var ks = make([]float64, intervals)
	for d := 0; d < days-L; d++ {
		for tau := 0; tau < n; tau++ {
			go func(s, tau int) {
				ks[s*n+tau] = limitK * math.Sqrt(math.Min(BVs[s], RVs[s])*TODs[s*n+tau])
				wg.Done()
			}(d, tau)
		}
	}
	wg.Wait()
	fmt.Println("GetKs Time consumed: ", time.Now().Sub(t).Seconds())

	return ks
}

func GetRs(df dataframe.DataFrame) []float64 {
	closePrices := df.Col("Close")
	openPrices := df.Col("Open")
	openPricesF := openPrices.Float()
	var rs []float64
	for i, closep := range closePrices.Float() {
		rs = append(rs, math.Log(closep)-math.Log(openPricesF[i]))
	}
	return rs
}

// BV = pi/2 * sum(|r_tau|*|r_{tau-1}|)
// of one day
func BV(rs []float64) float64 {
	var sum, former float64
	for _, r := range rs {
		sum += math.Abs(r) * math.Abs(former)
		former = r
	}
	sum *= math.Pi / 2
	return sum
}

// RV = sum(r^2)
// Input: array of r of one day
func RV(rs []float64) float64 {
	var sum float64
	for _, r := range rs {
		sum += math.Pow(r, 2)
	}
	return sum
}

// TOD = n*sum(r_tau^2) / sum(r)
// Input: array of r on days from s to t
// should be L days
func TodAtTau(rs []float64, tau int, n int, BVs, RVs []float64) float64 {
	var dividend, divisor, limit float64
	for i, r := range rs {
		if i%n == 0 {
			limit = limitK * math.Sqrt(math.Min(BVs[i/n], RVs[i/n])) //BV(rs[i:i+n])*RV(rs[i:i+n]))
		}
		if math.Abs(r) <= limit {
			r2 := math.Pow(r, 2)
			if i == tau {
				dividend += r2
			}
			divisor += r2
		}
	}
	tod := float64(n) * dividend / divisor
	return tod
}
