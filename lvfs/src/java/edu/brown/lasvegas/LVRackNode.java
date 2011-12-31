package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Rack node is a physical machine in a rack.
 */
@Entity
public class LVRackNode implements LVObject {
    
    /** The Constant IX_NAME. */
    public static final String IX_NAME = "IX_NAME";
    /**
     * A unique name of the node. Probably FQDN of the machine.
     */
    @SecondaryKey(name=IX_NAME, relate=Relationship.ONE_TO_ONE)
    private String name;

    /**
     * Unique ID of the node.
     */
    @PrimaryKey
    private int nodeId;
    
    /**
     * @see edu.brown.lasvegas.LVObject#getPrimaryKey()
     */
    @Override
    public int getPrimaryKey() {
        return nodeId;
    }
    
    /**
     * address string (host:port) to connect to the data node, such as "localhost:12345".
     */
    private String address;
    
    /** The Constant IX_RACK_ID. */
    public static final String IX_RACK_ID = "IX_RACK_ID";
    /** The rack at which this node is physically located. */
    @SecondaryKey(name=IX_RACK_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVRack.class)
    private int rackId;
    
    /**
     * Status of the node.
     */
    private RackNodeStatus status;
    
    @Override
    public String toString() {
        return "RackNode-" + nodeId + " (Name=" + name
        + ", Address=" + address + ",Status=" + status + ", RackId=" + rackId + ")";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(name == null);
        if (name != null) {
            out.writeUTF(name);
        }
        out.writeBoolean(address == null);
        if (address != null) {
            out.writeUTF(address);
        }
        out.writeInt(nodeId);
        out.writeInt(rackId);
        out.writeInt(status == null ? RackNodeStatus.INVALID.ordinal() : status.ordinal());
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            name = null;
        } else {
            name = in.readUTF();
        }
        if (in.readBoolean()) {
            address = null;
        } else {
            address = in.readUTF();
        }
        nodeId = in.readInt();
        rackId = in.readInt();
        status = RackNodeStatus.values()[in.readInt()];
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVRackNode read (DataInput in) throws IOException {
        LVRackNode obj = new LVRackNode();
        obj.readFields(in);
        return obj;
    }

    @Override
    public LVObjectType getObjectType() {
        return LVObjectType.RACK_NODE;
    }
    
// auto-generated getters/setters (comments by JAutodoc)

    /**
     * Gets the a unique name of the node.
     *
     * @return the a unique name of the node
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the a unique name of the node.
     *
     * @param name the new a unique name of the node
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the unique ID of the node.
     *
     * @return the unique ID of the node
     */
    public int getNodeId() {
        return nodeId;
    }

    /**
     * Sets the unique ID of the node.
     *
     * @param nodeId the new unique ID of the node
     */
    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Gets the rack at which this node is physically located.
     *
     * @return the rack at which this node is physically located
     */
    public int getRackId() {
        return rackId;
    }

    /**
     * Sets the rack at which this node is physically located.
     *
     * @param rackId the new rack at which this node is physically located
     */
    public void setRackId(int rackId) {
        this.rackId = rackId;
    }

    /**
     * Gets the status of the node.
     *
     * @return the status of the node
     */
    public RackNodeStatus getStatus() {
        return status;
    }

    /**
     * Sets the status of the node.
     *
     * @param status the new status of the node
     */
    public void setStatus(RackNodeStatus status) {
        this.status = status;
    }

    /**
     * Gets the address string (host:port) to connect to the data node, such as "localhost:12345".
     *
     * @return the address string (host:port) to connect to the data node, such as "localhost:12345"
     */
    public String getAddress() {
        return address;
    }

    /**
     * Sets the address string (host:port) to connect to the data node, such as "localhost:12345".
     *
     * @param address the new address string (host:port) to connect to the data node, such as "localhost:12345"
     */
    public void setAddress(String address) {
        this.address = address;
    }
}
