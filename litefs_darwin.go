package litefs

import (
	"fmt"
	"os"
)

func init() {
	fmt.Println("litefs not supported on macOS")
	os.Exit(1)
}
