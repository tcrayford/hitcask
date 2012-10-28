module Database.Hitcask.Hint where
import Database.Hitcask.Types
import Database.Hitcask.Put
import Database.Hitcask.Restore
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.UTF8 as U
import Data.Serialize.Put
import Data.Serialize.Get
import qualified Data.HashMap.Strict as M
import System.IO(hFlush)

writeHint :: HintFile -> Key -> ValueLocation -> IO ()
writeHint (LogFile h _) key loc = do
  B.hPut h (hint key loc)
  hFlush h

hint :: Key -> ValueLocation -> B.ByteString
hint key (ValueLocation f vs vp ts) = runPut $ do
  putInt32 vs
  putInt32 vp
  putInt32 ts
  putInt32 (length f)
  putByteString $ B.pack f
  putInt32 (B.length key)
  putByteString key

restoreFromHintFile :: HintFile -> IO KeyDir
restoreFromHintFile f = do
  let h = handle f
  wholeFile <- B.hGetContents h
  let r = runParser wholeFile $
            untilM isEmpty readLoc
  return $! M.fromList r

readLoc :: Get (Key, ValueLocation)
readLoc = do
  vs <- getInt32
  vp <- getInt32
  ts <- getInt32
  fpLength <- getInt32
  fp <- getByteString fpLength
  keyLength <- getInt32
  key <- getByteString keyLength
  return (key, ValueLocation (U.toString fp) vs (fromIntegral vp) (fromIntegral ts))

getInt32 :: Get Int
getInt32 = fmap fromIntegral getWord32be

