package rcd

import (
	"log"

	"github.com/ncw/rclone/cmd"
	"github.com/ncw/rclone/fs/rc"
	"github.com/ncw/rclone/fs/rc/rcflags"
	"github.com/spf13/cobra"
)

func init() {
	cmd.Root.AddCommand(commandDefintion)
}

var commandDefintion = &cobra.Command{
	Use:   "rcd <path to files to serve>*",
	Short: `Run rclone listening to remote control commands only.`,
	Long: `
This runs rclone so that it only listents to remote control commands.

This is useful if you are controlling rclone via the rc API.

If you pass in a path to a directory, rclone will serve that directory
for GET requests on the URL passed in.  It will also open the URL in
the browser when rclone is run.
`,
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(0, 1, command, args)
		if rcflags.Opt.Enabled {
			log.Fatalf("Don't supply --rc flag when using rcd")
		}
		// Start the rc
		rcflags.Opt.Enabled = true
		if len(args) > 0 {
			rcflags.Opt.Files = args[0]
		}
		rc.Start(&rcflags.Opt)
		// Run the rc forever
		select {}
	},
}