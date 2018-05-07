{-# LANGUAGE Arrows #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE FlexibleContexts #-}

module Main where

import Control.Arrow
import Control.Exception (Exception (..))
import Control.Funflow
import Data.Default
import Control.Funflow.ContentStore (Content (..))
import qualified Control.Funflow.ContentStore as CS
import qualified Control.Funflow.External.Docker as Docker
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Path
import Path.IO
import Conduit
import Data.Conduit.Combinators (sinkFile)
import Network.HTTP.Simple
import qualified Data.ByteString.Lazy as B
import Codec.Compression.GZip
import Text.RawString.QQ

main :: IO ()
main = do
    cwd <- getCurrentDir
    res <- withSimpleLocalRunner (cwd </> [reldir|funflow-example/store|]) $ \run -> do
      run mainFlow ()
    case res of
      Left err ->
        putStrLn $ "FAILED: " ++ displayException err
      Right _out ->
        putStrLn $ "SUCCESS: "


--  Utility Flows, these are general functions which could be used in other
--  projects

copyFromStore :: ArrowFlow eff ex arr => Path b1 File -> arr (Content File) ()
copyFromStore  dest = getFromStore (\p -> copyFile p dest)

copyDirFromStore :: ArrowFlow eff ex arr => Path b1 Dir -> arr (Content Dir) ()
copyDirFromStore dest = getFromStore (\p -> copyDirRecur p dest)

fileToDir :: SimpleFlow (Content File) (Content Dir)
fileToDir = proc (i :</> _) -> do
  returnA -< (CS.All i)

downloadFileToStore :: ArrowFlow eff ex arr => arr (String, Path Rel File) (CS.Content File)
downloadFileToStore = putInStoreAt $ \p url -> runResourceT $ httpSink (parseRequest_ url) (\_ -> (sinkFile (toFilePath p)))

untar :: ArrowFlow eff ex arr => arr B.ByteString B.ByteString
untar = step' (def { cache = defaultCacherWithIdent 1234 }) decompress

-- | Read the contents of the given file in the store.
readBS :: ArrowFlow eff ex arr => arr (CS.Content File) B.ByteString
readBS = getFromStore $ B.readFile . fromAbsFile

-- | Create and write into a file under the given path in the store.
writeBS :: ArrowFlow eff ex arr => arr (B.ByteString, Path Rel File) (CS.Content File)
writeBS = putInStoreAt $ B.writeFile . fromAbsFile

downloadAndUnzip :: SimpleFlow String (Content File)
downloadAndUnzip = proc url -> do
  raw_file <- downloadFileToStore -< (url, [relfile|raw|])
  utar <- untar <<< readBS -< raw_file
  writeBS -< (utar, [relfile|out|])

inpath :: Content File -> T.Text
inpath infile = T.pack (toFilePath ([absdir|/input/data/|] </> CS.contentFilename infile))

-- This flow implements the pipeline described in
-- http://www.qgistutorials.com/en/docs/processing_graphical_modeler.html
-- The flows are not parameterised as I didn't use a proper templating
-- language for the scripts and it seemed more effort than it was worth
mainFlow :: SimpleFlow () ()
mainFlow = proc () -> do
    data2001 <- downloadAndUnzip -< data2001URL
    _data2012 <- downloadAndUnzip -< data2012URL

    copyDirFromStore [absdir|/root/funflow/out|] <<<
      fileToDir <<< qgisExtractAttribute <<< polygonizeGDAL <<<
        majorityFilter <<< convertTifToSgrd -< data2001

data2001URL, data2012URL :: String
data2001URL = "http://www.qgistutorials.com/downloads/LC_hd_global_2001.tif.gz"
data2012URL = "http://www.qgistutorials.com/downloads/LC_hd_global_2012.tif.gz"


-- | Use io_gdal to convert a tif image for sgrd suitable for using with
-- SAGA
convertTifToSgrd :: SimpleFlow (CS.Content File) (CS.Content File)
convertTifToSgrd = proc file -> do
  scriptInput <- writeExecutableString -< (compileScript, [relfile|compile.sh|])
  sgrd <- sagaDocker -< (file, scriptInput, ["/output/out.sgrd", inpath file] )
  returnA -< sgrd :</> [relfile|out.sgrd|]

  where
    convertCmd = "io_gdal 0 -TRANSFORM 0 -RESAMPLING 0 -GRIDS \"$1\" -FILES \"$2\" "

    compileScript = sagaCmd convertCmd


sagaCmd :: String -> String
sagaCmd s = unlines $ ["#! /usr/bin/env bash"
                       , unwords ["saga_cmd", s] ]

gdalCmd :: String -> String
gdalCmd s = unlines $ ["#! /usr/bin/env bash"
                      , s]

-- | This flow invokes the SAGA majority filter
majorityFilter :: SimpleFlow (Content File) (Content File)
majorityFilter  = proc file -> do
    scriptInput <- writeExecutableString -< (compileScript, [relfile|compile.sh|])
    sdat <- sagaDocker  -< (file, scriptInput, ["/output/out.sdat", inpath file])
    returnA -< sdat :</> [relfile|out.sdat|]
  where
    majorityFilterScript = "grid_filter \"Majority Filter\" -INPUT $2 -MODE 0 -RADIUS 1 -THRESHOLD 0 -RESULT $1"
    compileScript = sagaCmd majorityFilterScript

-- | Call gdal_polygonize
polygonizeGDAL :: SimpleFlow (Content File) (Content File)
polygonizeGDAL  = proc (infile) -> do
  scriptInput <- writeExecutableString -< (compileScript, [relfile|compile.sh|])
  shp <- gdalDocker -< (infile, scriptInput, ["/output/out.shp", inpath infile])
  returnA -< shp :</> [relfile|out.shp|]
  where
    compileScript = gdalCmd "gdal_polygonize.py $2 -f \"ESRI Shapefile\" $1 OUTPUT DN"

-- | Call the "Extract by attribute" QGIS method
qgisExtractAttribute :: SimpleFlow (Content File) (Content File)
qgisExtractAttribute = proc infile -> do
  compileScript <- writeExecutableString -< (qgisScript, [relfile|run.py|])
  sdat <- qgisDocker  -< (infile, compileScript)
  returnA -< sdat :</> [relfile|out.shp|]

qgisWrapper :: String
qgisWrapper = [r|#! /usr/bin/env bash
export PYTHONPATH=/usr/share/qgis/python/
python /input/script/run.py
|]


qgisScript :: String
qgisScript = [r|
import sys
from qgis.core import *
from PyQt4.QtGui import *

app = QApplication(sys.argv, False)
QgsApplication.setPrefixPath("/usr", True)
QgsApplication.initQgis()

sys.path.append('/usr/share/qgis/python/plugins')

from processing.core.Processing import Processing
Processing.initialize()
from processing.tools import *

input = QgsVectorLayer('/input/data/out.shp', 'input', 'ogr')

general.runalg('qgis:extractbyattribute', input, "DN", 0, 12, "/output/out.shp")
|]


{-
Docker Images
-}

qgisDocker :: SimpleFlow (Content File, Content File) CS.Item
qgisDocker = proc (inf, s) -> do
  qwrap <- writeExecutableString -< (qgisWrapper, [relfile|wrapper.sh|])
  (docker $ \(infile, scriptInput, wrapper) -> Docker.Config
      { Docker.image = "kartoza/qgis-desktop"
      , Docker.optImageID = Just "2.18.17"
      , Docker.input = Docker.MultiInput
        $ Map.fromList [ ("script", IPItem $ CS.contentItem scriptInput)
                       , ("wrapper", IPItem $ CS.contentItem wrapper)
                       , ("data", IPItem $ CS.contentItem infile)
                       ]
      , Docker.command = "/input/wrapper/wrapper.sh"
      , Docker.args = []
      }) -< (inf, s, qwrap)

sagaDocker :: SimpleFlow (Content File, Content File, [T.Text]) CS.Item
sagaDocker = docker $ \(infile, scriptInput, args) -> Docker.Config
      { Docker.image = "saga"
      , Docker.optImageID = Nothing
      , Docker.input = Docker.MultiInput
        $ Map.fromList [ ("script", IPItem $ CS.contentItem scriptInput)
                       , ("data", IPItem $ CS.contentItem infile)
                       ]
      , Docker.command = "/input/script/compile.sh"
      , Docker.args = args
      }

gdalDocker :: SimpleFlow (Content File, Content File, [T.Text]) CS.Item
gdalDocker = docker $ \(infile, scriptInput, args) -> Docker.Config
      { Docker.image = "geodata/gdal"
      , Docker.optImageID = Nothing
      , Docker.input = Docker.MultiInput
        $ Map.fromList [ ("script", IPItem $ CS.contentItem scriptInput)
                       , ("data", IPItem $ CS.contentItem infile)
                       ]
      , Docker.command = "/input/script/compile.sh"
      , Docker.args = args
      }
