package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tursom/turntf-port-forward/internal/portforward"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "turntf-port-forward: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage(os.Stderr)
		return errors.New("missing command")
	}
	switch args[0] {
	case "server":
		path, err := configPath(args[1:])
		if err != nil {
			return err
		}
		cfg, err := portforward.LoadServerConfig(path)
		if err != nil {
			return err
		}
		runtime, err := portforward.NewServerRuntime(cfg, log.Default())
		if err != nil {
			return err
		}
		return runUntilSignal(runtime.Run)
	case "client":
		path, err := configPath(args[1:])
		if err != nil {
			return err
		}
		cfg, err := portforward.LoadClientConfig(path)
		if err != nil {
			return err
		}
		runtime, err := portforward.NewClientRuntime(cfg, log.Default())
		if err != nil {
			return err
		}
		return runUntilSignal(runtime.Run)
	case "check-config":
		if len(args) < 2 {
			return errors.New("check-config requires server or client")
		}
		path, err := configPath(args[2:])
		if err != nil {
			return err
		}
		switch args[1] {
		case "server":
			_, err = portforward.LoadServerConfig(path)
		case "client":
			_, err = portforward.LoadClientConfig(path)
		default:
			return errors.New("check-config requires server or client")
		}
		if err != nil {
			return err
		}
		fmt.Println("config ok")
		return nil
	case "example-config":
		if len(args) != 2 {
			return errors.New("example-config requires server or client")
		}
		switch args[1] {
		case "server":
			fmt.Print(portforward.ServerExampleConfig)
		case "client":
			fmt.Print(portforward.ClientExampleConfig)
		default:
			return errors.New("example-config requires server or client")
		}
		return nil
	case "help", "-h", "--help":
		usage(os.Stdout)
		return nil
	default:
		usage(os.Stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func configPath(args []string) (string, error) {
	flags := flag.NewFlagSet("config", flag.ContinueOnError)
	path := flags.String("c", "config.yaml", "配置文件路径")
	flags.StringVar(path, "config", "config.yaml", "配置文件路径")
	if err := flags.Parse(args); err != nil {
		return "", err
	}
	if flags.NArg() != 0 {
		return "", fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return *path, nil
}

func runUntilSignal(run func(context.Context) error) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := run(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func usage(out *os.File) {
	fmt.Fprintln(out, "用法:")
	fmt.Fprintln(out, "  turntf-port-forward server -c server.yaml")
	fmt.Fprintln(out, "  turntf-port-forward client -c client.yaml")
	fmt.Fprintln(out, "  turntf-port-forward check-config <server|client> -c config.yaml")
	fmt.Fprintln(out, "  turntf-port-forward example-config <server|client>")
}
