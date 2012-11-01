module Database.Hitcask.Timestamp where
import Data.Time.Clock.POSIX
import Data.List.Split(splitOn)

currentTimestamp :: IO Integer
currentTimestamp = do
  a <- getPOSIXTime
  return $! read (head (splitOn "s" (concat (splitOn "." (show a)))))

