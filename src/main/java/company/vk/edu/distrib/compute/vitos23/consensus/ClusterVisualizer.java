package company.vk.edu.distrib.compute.vitos23.consensus;

import javax.swing.*;
import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Line2D;
import java.io.Serial;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public final class ClusterVisualizer {

    private static final int NODE_RADIUS = 40;
    private static final int PANEL_PADDING = 50;
    private static final int REFRESH_MS = 200;
    private static final int WINDOW_SIZE = 1000;
    private static final int FONT_SIZE = 24;

    public void show(Map<Integer, Node> nodeById) {
        SwingUtilities.invokeLater(() -> {
            ClusterPanel panel = new ClusterPanel(nodeById);
            JFrame frame = new JFrame("Cluster Topology");
            frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
            frame.add(panel);
            frame.setSize(WINDOW_SIZE, WINDOW_SIZE);
            frame.setLocationRelativeTo(null);
            frame.setVisible(true);

            Timer timer = new Timer(REFRESH_MS, e -> panel.repaint());
            timer.start();
        });
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private static class ClusterPanel extends JPanel {

        @Serial
        private static final long serialVersionUID = 123123L;

        private final List<Node> nodes;

        ClusterPanel(Map<Integer, Node> nodeById) {
            super();
            this.nodes = nodeById.values().stream()
                    .sorted(Comparator.comparingInt(Node::getId))
                    .toList();
            setPreferredSize(new Dimension(WINDOW_SIZE, WINDOW_SIZE));
            setBackground(new Color(30, 30, 30));
            setFont(new Font(Font.SANS_SERIF, Font.BOLD, FONT_SIZE));
        }

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            Graphics2D g2 = (Graphics2D) g;
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

            int count = nodes.size();
            int centerX = getWidth() / 2;
            int centerY = getHeight() / 2;
            int layoutRadius = Math.min(getWidth(), getHeight()) / 2 - PANEL_PADDING - NODE_RADIUS;

            int[] xs = new int[count];
            int[] ys = new int[count];
            NodeStatus[] statuses = new NodeStatus[count];

            for (int i = 0; i < count; i++) {
                double angle = 2.0 * Math.PI * i / count - Math.PI / 2;
                xs[i] = centerX + (int) (layoutRadius * Math.cos(angle));
                ys[i] = centerY + (int) (layoutRadius * Math.sin(angle));
                statuses[i] = nodes.get(i).getStatus();
            }

            drawLines(g2, xs, ys, statuses);
            drawNodes(g2, xs, ys, statuses);
        }

        private void drawLines(Graphics2D g2, int[] xs, int[] ys, NodeStatus... statuses) {
            g2.setStroke(new BasicStroke(1.5f));
            int n = xs.length;
            for (int i = 0; i < n; i++) {
                if (statuses[i] == NodeStatus.DOWN) {
                    continue;
                }
                for (int j = i + 1; j < n; j++) {
                    if (statuses[j] == NodeStatus.DOWN) {
                        continue;
                    }
                    g2.setColor(new Color(100, 100, 100));
                    g2.draw(new Line2D.Float(xs[i], ys[i], xs[j], ys[j]));
                }
            }
        }

        private void drawNodes(Graphics2D g2, int[] xs, int[] ys, NodeStatus... statuses) {
            FontMetrics fm = g2.getFontMetrics();
            int n = xs.length;
            for (int i = 0; i < n; i++) {
                Color fill = switch (statuses[i]) {
                    case LEADER -> new Color(34, 139, 34);
                    case FOLLOWER -> new Color(65, 105, 225);
                    case DOWN -> new Color(178, 34, 34);
                };
                Color border = fill.brighter();

                int drawX = xs[i] - NODE_RADIUS;
                int drawY = ys[i] - NODE_RADIUS;
                int diameter = 2 * NODE_RADIUS;

                g2.setColor(fill);
                g2.fill(new Ellipse2D.Float(drawX, drawY, diameter, diameter));
                g2.setColor(border);
                g2.setStroke(new BasicStroke(2.5f));
                g2.draw(new Ellipse2D.Float(drawX, drawY, diameter, diameter));

                String label = String.valueOf(nodes.get(i).getId());
                g2.setColor(Color.WHITE);
                int textWidth = fm.stringWidth(label);
                g2.drawString(
                        label,
                        xs[i] - textWidth / 2,
                        ys[i] + fm.getAscent() / 2 - 1
                );
            }
        }
    }
}
