package main

import (
	"fmt"
	"gokvs"
	"log"

	"github.com/spf13/cobra"
)

const (
	ADDRESS = "127.0.0.1:9999"
)

func main() {
	client, err := gokvs.NewClient(ADDRESS)
	if err != nil {
		log.Fatal(err)
	}
	cmdSet := &cobra.Command{
		Use:   "set",
		Short: "Set key to hold the string value",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rsp, err := client.Set(args[0], args[1])
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(rsp)
		},
	}
	cmdGet := &cobra.Command{
		Use:   "get",
		Short: "Get the value of key",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rsp, err := client.Get(args[0])
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(rsp)
		},
	}
	cmdDel := &cobra.Command{
		Use:   "del",
		Short: "Delete the value of key",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rsp, err := client.Del(args[0])
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(rsp)
		},
	}
	var rootCmd = &cobra.Command{Use: "kvs-cli"}
	rootCmd.AddCommand(cmdSet, cmdGet, cmdDel)
	err = rootCmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}
