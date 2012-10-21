module Database.Hitcask.Timestamp where
import System.Time

secondsToPicoseconds :: Integer -> Integer
secondsToPicoseconds s = s * (10 ^ 12)

currentTimestamp :: IO Integer
currentTimestamp = do
  (TOD s p) <- getClockTime
  let time = secondsToPicoseconds s + p
  return $! time
