module Database.Hitcask.Timestamp where
import Data.Time.Clock.POSIX

currentTimestamp :: IO Integer
currentTimestamp = do
  time <- getPOSIXTime
  return $! round time
