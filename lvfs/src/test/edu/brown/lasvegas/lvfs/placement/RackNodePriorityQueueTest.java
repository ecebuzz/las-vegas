package edu.brown.lasvegas.lvfs.placement;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.RackNodeStatus;

/** testcases for {@link RackNodePriorityQueue}. */
public class RackNodePriorityQueueTest {
    private RackNodePriorityQueue queue;
    
    LVRackNode getDummyNode (int id) {
        LVRackNode node = new LVRackNode();
        node.setNodeId(id);
        node.setStatus(RackNodeStatus.OK);
        node.setName("dummy-" + id);
        node.setRackId(1);
        return node;
    }

    @Before
    public void setUp () {
        // assume the following scenario
        // Node 1: currently stores 5 nodes in total.
        // Node 2: currently stores 0 nodes in total.
        // Node 3: currently stores 7 nodes in total.
        // Node 4: currently stores 6 nodes in total.
        ArrayList<RackNodeUsage> nodes = new ArrayList<RackNodeUsage>();
        nodes.add(new RackNodeUsage(getDummyNode(1), 5));
        nodes.add(new RackNodeUsage(getDummyNode(2), 0));
        nodes.add(new RackNodeUsage(getDummyNode(3), 7));
        nodes.add(new RackNodeUsage(getDummyNode(4), 6));
        queue = new RackNodePriorityQueue(nodes);
    }

    @After
    public void tearDown() {
        queue = null;
    }
    
    /** simply test the balancing property, so move on to next partition every time (such as only one replica scheme). */
    @Test
    public void testBalance () {
        int[] answers = new int[] {
            2, 2, 2, 2, 2,
            1, // now node 1 and node 2 become tie. as node 1 has smaller ID, node 1 should be picked.
            2, // then node 2.
            1, 2, 4, // now node 1, 2 and 4 become tie.
            1, 2, 3, 4// now node 1, 2, 3 and 4 become tie.
            
        };
        for (int i = 0; i < answers.length; ++i) {
            queue.moveToNextPartition(new ArrayList<Integer>()); // clear every time
            LVRackNode picked = queue.pickNode();
            assertEquals("answer[" + i + "] was different", answers[i], picked.getNodeId());
        }
    }

    /** test the property to avoid assigning multiple replica partitions of same ranges into same node. */
    @Test
    public void testDuplicate () {
        LVRackNode picked;

        queue.moveToNextPartition(new ArrayList<Integer>());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(1, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(4, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(3, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId()); // now all of them are already assigned, so the most vacant is picked
    }

    @Test
    public void testDuplicate2 () {
        LVRackNode picked;

        queue.moveToNextPartition(Arrays.asList(1, 3));
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(4, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(1, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
    }

    @Test
    public void testDuplicateCombined () {
        LVRackNode picked;

        queue.moveToNextPartition(Arrays.asList(1, 3));
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(4, picked.getNodeId());

        queue.moveToNextPartition(Arrays.asList(1, 4));
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(3, picked.getNodeId());

        queue.moveToNextPartition(new ArrayList<Integer>());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(1, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(4, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(3, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());

        queue.moveToNextPartition(Arrays.asList(3, 4));
        picked = queue.pickNode();
        assertEquals(1, picked.getNodeId());
        picked = queue.pickNode();
        assertEquals(2, picked.getNodeId());
    }
}
