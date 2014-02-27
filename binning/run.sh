#!/bin/bash
 
# source the ciop functions (e.g. ciop-log)
source ${ciop_job_include}
 
export BEAM_HOME=/usr/lib/esa/beam-4.11
export PATH=$BEAM_HOME/bin:$PATH
 
# define the exit codes
SUCCESS=0
ERR_NOINPUT=1
ERR_BEAM=2
ERR_NOPARAMS=5
 
 
# add a trap to exit gracefully
function cleanExit ()
{
   local retval=$?
   local msg=""
   case "$retval" in
     $SUCCESS)      msg="Processing successfully concluded";;
     $ERR_NOPARAMS) msg="Expression not defined";;
     $ERR_BEAM)    msg="Beam failed to process product $product (Java returned $res).";;
     *)             msg="Unknown error";;
   esac
   [ "$retval" != "0" ] && ciop-log "ERROR" "Error $retval - $msg, processing aborted" || ciop-log "INFO" "$msg"
   exit $retval
}
trap cleanExit EXIT
 
# create the output folder to store the output products
mkdir -p $TMPDIR/expr_output
export EXPR_DIR=$TMPDIR/expr_output
 
# retrieve the parameters value from workflow or job default value
expression="`ciop-getparam expression`"
 
# run a check on the expression value, it can't be empty
[ -z "$expression" ] && exit $ERR_NOPARAMS
 
 
# loop and process all MERIS products
while read inputfile 
do
	# report activity in log
	ciop-log "INFO" "Retrieving $inputfile from storage"
 
	# retrieve the remote geotiff product to the local temporary folder
	retrieved=`ciop-copy -o $TMPDIR $inputfile`
	
	# check if the file was retrieved
	[ "$?" == "0" -a -e "$retrieved" ] || exit $ERR_NOINPUT
	
	# report activity
	ciop-log "INFO" "Retrieved `basename $retrieved`, moving on to expression"
	outputname=`basename $retrieved`
 
	BEAM_REQUEST=$TMPDIR/beam_request.xml
cat << EOF > $BEAM_REQUEST
<?xml version="1.0" encoding="UTF-8"?>
<graph>
  <version>1.0</version>
  <node id="1">
    <operator>Read</operator>
      <parameters>
        <file>$retrieved</file>
      </parameters>
  </node>
  <node id="2">
    <operator>BandMaths</operator>
    <sources>
      <source>1</source>
    </sources>
    <parameters>
      <targetBands>
        <targetBand>
          <name>out</name>
          <expression>$expression</expression>
          <description>Processed Band</description>
          <type>float32</type>
        </targetBand>
      </targetBands>
    </parameters>
  </node>
  <node id="write">
    <operator>Write</operator>
    <sources>
       <source>2</source>
    </sources>
    <parameters>
      <file>$EXPR_DIR/$outputname</file>
   </parameters>
  </node>
</graph>
EOF
    ciop-log "INFO" "gpt.sh"
    gpt.sh $BEAM_REQUEST 1>&2 
   res=$?
   ciop-log "DEBUG" "gpt.sh returned $res"
   [ $res != 0 ] && exit $ERR_BEAM
 
    rm -f $retrieved 
done
 
function getVal() {
cat $1 | grep $2 | cut -d '>' -f 2 | cut -d '<' -f 1
}
 
function getValue() {
cat $1 | grep $2 | cut -d '"' -f 2 | cut -d '"' -f 1
}
 
# retrieve the parameters value from workflow or job default value
cellsize="`ciop-getparam cellsize`"
bandname="`ciop-getparam bandname`"
bitmask="`ciop-getparam bitmask`"
bbox="`ciop-getparam bbox`"
algorithm="`ciop-getparam algorithm`"
outputname="`ciop-getparam outputname`"
compress="`ciop-getparam compress`"
band="`ciop-getparam band`"
tailor="`ciop-getparam tailor`"
 
# run a check on the format value, it can't be empty
#[ -z "$reflecAs" ] || [ -z "$normReflec" ] || [ -z "$cloudIceExpr" ] && exit $ERR_NOPARAMS
 
xmin=`echo $bbox | cut -d "," -f 1`
ymin=`echo $bbox | cut -d "," -f 2`
xmax=`echo $bbox | cut -d "," -f 3`
ymax=`echo $bbox | cut -d "," -f 4`

ciop-log "DEBUG" "bbox: $xmin, $ymin, $xmax, $ymax"
 
l3db=$TMPDIR/l3_database.bindb
file=$TMPDIR/binning_request.xml
 
mkdir -p $TMPDIR/binning_output
 
# first part of request file
cat > $file << EOF
<?xml version="1.0" encoding="ISO-8859-1"?>
<RequestList>
<Request type="BINNING">
<Parameter name="process_type" value="init" />
<Parameter name="database" value="$l3db" />
<Parameter name="lat_min" value="$ymin" />
<Parameter name="lat_max" value="$ymax" />
<Parameter name="lon_min" value="$xmin" />
<Parameter name="lon_max" value="$xmax" />
<Parameter name="log_prefix" value="l3" />
<Parameter name="log_to_output" value="false" />
<Parameter name="resampling_type" value="binning" />
<Parameter name="grid_cell_size" value="$cellsize" />
<Parameter name="band_name.0" value="$bandname" />
<Parameter name="bitmask.0" value="$bitmask" />
<Parameter name="binning_algorithm.0" value="$algorithm" />
<Parameter name="weight_coefficient.0" value="1" />
</Request>
<Request type="BINNING">
<Parameter name="process_type" value="update" />
<Parameter name="database" value="$l3db" />
<Parameter name="log_prefix" value="l3" />
<Parameter name="log_to_output" value="false" />
EOF
 
for myfile in `find $EXPR_DIR -type f -name "*.dim"`
do
	ciop-log "DEBUG" "adding $myfile to binning request"
echo " <InputProduct URL=\"file://$myfile\" /> " >> $file
done
cat >> $file << EOF
</Request>
<Request type="BINNING">
<Parameter name="process_type" value="finalize" />
<Parameter name="database" value="$l3db" />
<Parameter name="delete_db" value="true" />
<Parameter name="log_prefix" value="l3" />
<Parameter name="log_to_output" value="false" />
<Parameter name="tailor" value="$tailor" />
<OutputProduct URL="file:$TMPDIR/binning_output/$outputname.dim" format="BEAM-DIMAP" />
</Request>
</RequestList>
EOF

ciop-log "DEBUG" "binning"
cp $file /tmp
binning.sh $file 1>&2
[ "$?" == "0" ] || exit $ERR_BINNING
 
ciop-log "INFO" "Publishing binned DIMAP product"
ciop-publish -m $TMPDIR/binning_output/$outputname.dim
ciop-publish -r -m $TMPDIR/binning_output/$outputname.data
 
cat > $TMPDIR/palette.cpd << EOF
`ciop-getparam "palette"`
EOF
 
ciop-log "INFO" "Generating image files"
pconvert.sh -f png -b $band $TMPDIR/binning_output/$outputname.dim -c $TMPDIR/palette.cpd -o $TMPDIR/binning_output >&2
[ "$?" == "0" ] || exit $ERR_PCONVERT
 
ciop-publish -m $TMPDIR/output/$outputname.png
pconvert.sh -f tif -b $band $TMPDIR/binning_output/$outputname.dim -c $TMPDIR/palette.cpd -o $TMPDIR/binning_output >&2
[ "$?" == "0" ] || exit $ERR_PCONVERT
mv $TMPDIR/binning_output/$outputname.tif $TMPDIR/binning_output/$outputname.rgb.tif
ciop-publish -m $TMPDIR/binning_output/$outputname.rgb.tif
pconvert.sh -f tif -b $band $TMPDIR/binning_output/$outputname.dim -o $TMPDIR/binning_output >&2
[ "$?" == "0" ] || exit $ERR_PCONVERT
ciop-publish -m $TMPDIR/binning_output/$outputname.tif
 
dim=$TMPDIR/binning_output/$outputname.dim
width=`getVal $dim NCOLS`
height=`getVal $dim NROWS`
 
minx=`getValue $dim EASTING`
maxy=`getValue $dim NORTHING`
resx=`getValue $dim PIXELSIZE_X`
resy=`getValue $dim PIXELSIZE_Y`
 
maxx=`echo "$minx + $width * $resx" | bc -l `
miny=`echo "$maxy - $height * $resy" | bc -l `	
 
convert -cache 1024 -size ${width}x${height} -depth 8 -interlace Partition $TMPDIR/binning_output/$outputname.png $TMPDIR/tmp.jpeg >&2
[ "$?" == "0" ] || exit $ERR_JPEGTMP
ciop-log "INFO" "Generating the browse"
convert -cache 1024 -size 150x150 -depth 8 -interlace Partition $TMPDIR/tmp.jpeg $TMPDIR/binning_output/${outputname}_browse.jpg >&2
[ "$?" == "0" ] || exit $ERR_BROWSE
ciop-publish -m $TMPDIR/binning_output/${outputname}_browse.jpg
 
 
exit 0
