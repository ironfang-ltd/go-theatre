package theatre

import (
	"log/slog"
	"os"
)

// InitLogger configures the global slog logger to output structured JSON
// to stderr. Call this once at program startup before creating any hosts.
// The level controls the minimum log level (e.g. slog.LevelInfo, slog.LevelDebug).
func InitLogger(level slog.Level) {
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})
	slog.SetDefault(slog.New(handler))
}
