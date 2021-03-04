package archive

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/spf13/cobra"
)

var _ = tar.FormatGNU

func init() {
	cmd.Root.AddCommand(commandDefinition)
}

var commandDefinition = &cobra.Command{
	Use:   "archive remote:path",
	Short: `Archive any files as tar and sends them to stdout.`,
	// Warning! "|" will be replaced by backticks below
	Long: strings.ReplaceAll(`
rclone archive tar any files to standard output.

You can use it like this to output a single file

    rclone archive remote:path/to/file

Or like this to output any file in dir or its subdirectories.

    rclone archive remote:path/to/dir

Or like this to output any .txt files in dir or its subdirectories.

    rclone --include "*.txt" archive remote:path/to/dir
`, "|", "`"),
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(1, 1, command, args)
		fsrc := cmd.NewFsSrc(args)

		cmd.Run(false, false, command, func() error {
			archive := tar.NewWriter(os.Stdout)
			defer archive.Close()
			ctx := context.Background()
			var mu sync.Mutex
			ci := fs.GetConfig(ctx)

			return operations.ListFn(ctx, fsrc, func(o fs.Object) {
				var options []fs.OpenOption
				for _, option := range ci.DownloadHeaders {
					options = append(options, option)
				}

				in, err := o.Open(ctx, options...)
				if err != nil {
					err = fs.CountError(err)
					fs.Errorf(o, "Failed to open: %v", err)
					return
				}

				mu.Lock()
				defer mu.Unlock()

				h := &tar.Header{
					Typeflag:   tar.TypeReg,
					Name:       o.String(),
					Size:       o.Size(),
					Mode:       0644,
					ModTime:    o.ModTime(ctx),
					AccessTime: o.ModTime(ctx),
					ChangeTime: o.ModTime(ctx),
					Format:     tar.FormatPAX,
				}

				archive.WriteHeader(h)
				_, err = io.Copy(archive, in)
				if err != nil {
					err = fs.CountError(err)
					fs.Errorf(o, "Failed to write: %v", err)
					return
				}
			})
		})
	},
}
