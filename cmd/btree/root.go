/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
package btree

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/seipan/btree/btree"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "btree",
	Short: "A brief description of your application",
	Long:  ``,

	Run: func(cmd *cobra.Command, args []string) {
		key, err := cmd.Flags().GetString("N")
		if err != nil {
			log.Fatal(err)
		}
		mdp := btree.NewDefaultdb()
		defer mdp.Close()

		n, err := strconv.Atoi(key)
		if err != nil {
			log.Fatal(err)
		}

		btr := btree.New(n)
		timedp := MeasurerDMP(n, mdp, SetMap)
		log.Println(timedp)
		timedp = MeasurerDMP(n, mdp, GetMap)
		log.Println(timedp)

		timebtr := MeasurerBtree(n, btr, SetBtree)
		log.Println(timebtr)
		timebtr = MeasurerBtree(n, btr, GetBtree)
		log.Println(timebtr)

	},
}

func SetMap(N int, mdp *btree.Defaultdb) {
	fmt.Println("--------------------------- default map create ---------------------------")
	for i := 0; i < N; i++ {
		mdp.Set(strconv.Itoa(i), strconv.Itoa(i))
	}
	fmt.Println("--------------------------- default map create ---------------------------")
}

func GetMap(N int, mdp *btree.Defaultdb) {
	fmt.Println("--------------------------- default map get ---------------------------")
	mdp.GetValue(strconv.Itoa(N - 2))
	fmt.Println("--------------------------- default map get ---------------------------")
}

func SetBtree(N int, btr *btree.BTree) {
	fmt.Println("--------------------------- btree create ---------------------------")
	for i := 0; i < N; i++ {
		btr.ReplaceOrInsert(btree.Int(i))
	}
	fmt.Println("--------------------------- btree create ---------------------------")
}

func GetBtree(N int, btr *btree.BTree) {
	fmt.Println("--------------------------- btree get ---------------------------")
	btr.Get(btree.Int(N - 2))
	fmt.Println("--------------------------- btree get ---------------------------")
}

func MeasurerDMP(N int, mdp *btree.Defaultdb, fnc func(N int, mdp *btree.Defaultdb)) time.Duration {
	start := time.Now()
	fnc(N, mdp)
	end := time.Now()
	return end.Sub(start)
}

func MeasurerBtree(N int, btr *btree.BTree, fnc func(N int, btr *btree.BTree)) time.Duration {
	start := time.Now()
	fnc(N, btr)
	end := time.Now()
	return end.Sub(start)
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringP("N", "N", "", "number of keys in the tree")
}
