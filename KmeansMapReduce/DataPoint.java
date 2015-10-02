

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;




public class DataPoint implements WritableComparable{

	Text artistHotness;
	Text beat;
	Text loudness;
	Text songID;


	public DataPoint(Text row)
	{
		String[] rowdata = row.toString().split(",");
		this.songID = new Text(rowdata[0]);
		this.artistHotness = new Text(rowdata[1]);
		this.beat = new Text(rowdata[8]);
		this.loudness = new Text(rowdata[7]);
	}
	
	
	public DataPoint(String row)
	{
		String[] rowdata = row.split(",");
		this.songID = new Text(rowdata[0]);
		this.artistHotness = new Text(rowdata[1]);
		this.beat = new Text(rowdata[8]);
		this.loudness = new Text(rowdata[7]);
		
	}
	
	public DataPoint(String id, String hotness, String loud, String beats)
	{
		this.songID = new Text(id);
		this.artistHotness = new Text(hotness);
		this.beat = new Text(beats);
		this.loudness = new Text(loud);
		
	}
	
	
	public double getArtistHotnessinDouble(){
		String artisthotness = artistHotness.toString();
		return Double.parseDouble(artisthotness);
	}
	
	public double getBeatinDouble(){
		String Beat = beat.toString();
		return Double.parseDouble(Beat);
	}
	public double getLoudnessinDouble(){
		String Loudness = loudness.toString();
		return Double.parseDouble(Loudness);
	}
	
	
	
	public double Distance(DataPointKey second){
		double distance  = 0.0;
		
		distance = Math.sqrt(
				Math.pow(this.getArtistHotnessinDouble() - second.getArtistHotnessinDouble(), 2.0) + 
				Math.pow(this.getBeatinDouble() - second.getBeatinDouble(), 2.0)+
				Math.pow(this.getLoudnessinDouble() - second.getLoudnessinDouble(), 2.0)
				);
		
		
		
		return distance;
	}


	public Text getArtistHotness() {
		return artistHotness;
	}


	public void setArtistHotness(Text artistHotness) {
		this.artistHotness = artistHotness;
	}


	public Text getBeat() {
		return beat;
	}


	public void setBeat(Text beat) {
		this.beat = beat;
	}


	public Text getLoudness() {
		return loudness;
	}


	public void setLoudness(Text loudness) {
		this.loudness = loudness;
	}
	
	public Text getSongID() {
		return songID;
	}


	public void setSongID(Text songID) {
		this.songID = songID;
	}


	@Override
	public String toString() {
		return getSongID()+ "," +getArtistHotness()+ "," + getLoudness()+ "," + getBeat();
	}


	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.artistHotness = new Text(arg0.readUTF());
		this.loudness = new Text(arg0.readUTF());
		this.beat = new Text(arg0.readUTF());
		this.songID = new Text(arg0.readUTF());
	}


	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(this.artistHotness.toString());
		arg0.writeUTF(this.loudness.toString());
		arg0.writeUTF(this.beat.toString());	
		arg0.writeUTF(this.songID.toString());
	}


	@Override
	public int compareTo(Object o) {
		return this.songID.compareTo(((DataPoint)o).getSongID());
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((artistHotness == null) ? 0 : artistHotness.hashCode());
		result = prime * result + ((beat == null) ? 0 : beat.hashCode());
		result = prime * result
				+ ((loudness == null) ? 0 : loudness.hashCode());
		result = prime * result + ((songID == null) ? 0 : songID.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataPoint other = (DataPoint) obj;
		if (artistHotness == null) {
			if (other.artistHotness != null)
				return false;
		} else if (!artistHotness.equals(other.artistHotness))
			return false;
		if (beat == null) {
			if (other.beat != null)
				return false;
		} else if (!beat.equals(other.beat))
			return false;
		if (loudness == null) {
			if (other.loudness != null)
				return false;
		} else if (!loudness.equals(other.loudness))
			return false;
		if (songID == null) {
			if (other.songID != null)
				return false;
		} else if (!songID.equals(other.songID))
			return false;
		return true;
	}


	public DataPoint() {
		super();
	}
	
}
