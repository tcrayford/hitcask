{-# LANGUAGE OverloadedStrings #-}
module Database.Hitcask.Benchmarks where
import Criterion.Main
import Database.Hitcask
import Database.Hitcask.SpecHelper

main :: IO ()
main = do
  p <- createEmpty "/tmp/hitcask/bench.measurePut"
  g <- createEmpty "/tmp/hitcask/bench.measureGet"
  put g "key" "value"
  defaultMain [
      bench "measurePut" $ measurePut p
    , bench "measureGet" $ measureGet g
    ]
  close p
  close g

measurePut :: Hitcask -> IO ()
measurePut h = do
  put h "key" "value"
  return ()

measureGet :: Hitcask -> IO ()
measureGet h = do
  get h "key"
  return ()

