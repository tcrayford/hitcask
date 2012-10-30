module Database.Hitcask.Parsing where
import qualified Data.ByteString.Char8 as B
import Database.Hitcask.Util
import Data.Serialize.Get

parseMany :: B.ByteString -> Get a -> [a]
parseMany input parser = runParser input $
  untilM isEmpty parser

runParser :: B.ByteString -> Get a -> a
runParser input g = case runGet g input of
  Right r -> r
  Left e -> error e

