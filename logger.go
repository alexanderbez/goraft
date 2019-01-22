package goraft

import (
	"io"
	"time"

	"go.uber.org/zap/zapcore"

	"go.uber.org/zap"
)

type (
	// Logger defines a logging interface a Raft node expects to use.
	Logger interface {
		Info(msg string, args ...interface{})
		Debug(msg string, args ...interface{})
		Error(msg string, args ...interface{})
		With(args ...interface{}) Logger
	}

	// raftLogger defines a default Raft logger using zap as the underlying
	// logging library.
	raftLogger struct {
		zapLogger *zap.SugaredLogger
	}
)

func NewRaftLogger(w io.Writer) Logger {
	// create a zap WriteSyncer based on the provided io.Writer
	ws := zapcore.AddSync(w)
	cfg := zap.NewProductionConfig()

	// create a zap encoder config and override time key and encoder
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "time"
	encCfg.EncodeTime = rfc3339TimeEncoder

	enc := zapcore.NewJSONEncoder(encCfg)
	core := zapcore.NewCore(enc, ws, cfg.Level)

	// set logging error output and sampling options (based on zap production)
	opts := []zap.Option{
		zap.ErrorOutput(ws),
		zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return zapcore.NewSampler(
				core, time.Second, cfg.Sampling.Initial, cfg.Sampling.Thereafter,
			)
		}),
	}

	rl := &raftLogger{zap.New(core, opts...).Sugar()}
	defer rl.zapLogger.Sync() // nolint: errcheck

	return rl
}

func (rl *raftLogger) Info(msg string, args ...interface{}) {
	rl.zapLogger.Infow(msg, args...)
}

func (rl *raftLogger) Debug(msg string, args ...interface{}) {
	rl.zapLogger.Debugw(msg, args...)
}

func (rl *raftLogger) Error(msg string, args ...interface{}) {
	rl.zapLogger.Errorw(msg, args...)
}

func (rl *raftLogger) With(args ...interface{}) Logger {
	rl.zapLogger = rl.zapLogger.With(args)
	return rl
}

func rfc3339TimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.UTC().Format(time.RFC3339))
}
