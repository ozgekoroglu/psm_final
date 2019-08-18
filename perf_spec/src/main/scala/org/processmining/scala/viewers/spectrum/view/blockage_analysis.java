package org.processmining.scala.viewers.spectrum.view;

import org.processmining.scala.log.utils.common.csv.common.CsvExportHelper;
import org.processmining.scala.log.utils.common.csv.common.CsvImportHelper;

import javax.swing.*;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableRowSorter;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

public class blockage_analysis extends JFrame {
    private JPanel panel1;
    private JTable table1;
    private JTextField textField1;
    private JTextField textField2;
    private JPanel panel2;
    private JPanel panel3;
    private JPanel panel4;
    private static String segment;
    private static String date;
    private static TableRowSorter<DefaultTableModel> sorter;
    static List<RowFilter<Object, Object>> filters;

    public blockage_analysis(String _segment, String _date) throws FileNotFoundException {
        segment = _segment;
        date = _date;
        $$$setupUI$$$();
        this.panel1 = new JPanel();
        this.sorter = new TableRowSorter<DefaultTableModel>();
        this.textField1.setText(segment);
        this.textField2.setText(date);

    }

    public static void main(String[] args) {
        try {
            String datafile = "C:\\Users\\nlokor\\Desktop\\Outlier_Detection\\blockage_psm_general\\Friday.csv";
            FileReader fin = new FileReader(datafile);
            DefaultTableModel m = createTableModel(fin, null);

            JFrame f = new JFrame();


            blockage_analysis window = new blockage_analysis(segment, date);

            window.table1 = new JTable(m);
            window.sorter = new TableRowSorter<DefaultTableModel>(m);

            window.panel1.add(window.table1);
            window.panel1.add(window.panel4);
            window.table1.setRowSorter(sorter);

            window.table1.addMouseListener(new MouseAdapter() {
                @Override
                public void mouseReleased(MouseEvent e) {
                    JTable table = (JTable) e.getSource();
                    int row = table.getSelectedRow();
                    //int row = table.rowAtPoint(new Point(0, e.getY() - table.getLocation().y)) - 2;
                    String segmentName = table.getValueAt(row, 0) + "";
                    String start_date = table.getValueAt(row, 5) + "";
                    String end_date = table.getValueAt(row, 6) + "";
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    String newStart;
                    String newEnd;
                    Date d_start = null;
                    Date d_end = null;
                    try {
                        d_start = sdf.parse(start_date);
                        d_end = sdf.parse(start_date);
                    } catch (ParseException ex) {
                        ex.printStackTrace();
                    }
                    sdf.applyPattern("dd-MM-yyyy HH:mm:ss.SSS");
                    newStart = sdf.format(d_start);
                    newEnd = sdf.format(d_end);


                    //MainPanel k = new MainPanel(null, null, false, null);

                    final CsvImportHelper helper = new CsvImportHelper(CsvExportHelper.FullTimestampPattern(), CsvExportHelper.AmsterdamTimeZone());
                    final long startUtcTimeMs = helper.extractTimestamp(newStart);
                    final long endUtcTimeMs = helper.extractTimestamp(newEnd);
                    final String segmentFilteringRegEx = segmentName;
                    MainPanel.segment_name = segmentFilteringRegEx;
                    MainPanel.start_date = startUtcTimeMs;
                    MainPanel.end_date = endUtcTimeMs;
                    refClassObject.findSegment(segmentFilteringRegEx, startUtcTimeMs, endUtcTimeMs);

                }
            });

            DecimalFormat df = new DecimalFormat("####0.00");
            df.setGroupingUsed(false);
            for (int i = 1; i < window.table1.getRowCount(); i++) {

                Object blockage_duration = window.table1.getModel().getValueAt(i, 2);
                String blockage_duration_value = String.valueOf(blockage_duration).trim().replaceAll("\"", "");
                Double new_value_blockage_duration = Double.parseDouble(df.format(Double.valueOf(blockage_duration_value)).replace(',', '.'));
                window.table1.getModel().setValueAt(new_value_blockage_duration, i, 2);

                Object avg_duration = window.table1.getModel().getValueAt(i, 4);
                String total_blockage_time_value = String.valueOf(avg_duration).trim().replaceAll("\"", "");
                Double new_value_avg_duration = Double.parseDouble(df.format(Double.valueOf(total_blockage_time_value)).replace(',', '.'));
                window.table1.getModel().setValueAt(new_value_avg_duration, i, 4);
            }

            filters = new ArrayList<RowFilter<Object, Object>>(2);
            filters.add(RowFilter.regexFilter("(?i)" + segment.toString(), 0));
            filters.add(RowFilter.regexFilter("(?i)" + date.toString(), 7));
            RowFilter<Object, Object> combined_filter = RowFilter.andFilter(filters);
            sorter.setRowFilter(combined_filter);

            window.table1.getTableHeader().setFont(new Font("Arial", Font.BOLD, 12));
            window.table1.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
            window.panel1.add(new JScrollPane(window.table1));

            f.getContentPane().add(new JScrollPane(window.table1), BorderLayout.CENTER);
            f.getContentPane().add(window.panel4, BorderLayout.NORTH);
            f.setExtendedState(JFrame.MAXIMIZED_BOTH);
            f.setVisible(true);
            f.pack();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DefaultTableModel createTableModel(Reader in,
                                                     Vector<Object> headers) {
        DefaultTableModel model = null;
        Scanner s = null;


        try {
            Vector<Vector<Object>> rows = new Vector<Vector<Object>>();
            s = new Scanner(in);

            while (s.hasNextLine()) {
                rows.add(new Vector<Object>(Arrays.asList(s.nextLine()
                        .split("\\s*,\\s*",
                                -1))));
            }

            if (headers == null) {
                headers = rows.get(0);
                model = new DefaultTableModel(rows, headers) {
                    @Override
                    public Class getColumnClass(int column) {
                        switch (column) {
                            case 0:
                                return String.class;
                            case 1:
                                return String.class;
                            case 2:
                                return Double.class;
                            case 3:
                                return Integer.class;
                            case 4:
                                return Double.class;
                            case 5:
                                return String.class;
                            case 6:
                                return String.class;
                            case 7:
                                return String.class;
                            default:
                                return String.class;
                        }
                    }
                };
            } else {
                model = new DefaultTableModel(rows, headers) {
                    @Override
                    public Class getColumnClass(int column) {
                        switch (column) {
                            case 0:
                                return String.class;
                            case 1:
                                return String.class;
                            case 2:
                                return Double.class;
                            case 3:
                                return Integer.class;
                            case 4:
                                return Double.class;
                            case 5:
                                return String.class;
                            case 6:
                                return String.class;
                            case 7:
                                return String.class;
                            default:
                                return String.class;
                        }
                    }
                };
            }

            return model;
        } finally {
            s.close();
        }
    }

    /**
     * Method generated by IntelliJ IDEA GUI Designer
     * >>> IMPORTANT!! <<<
     * DO NOT edit this method OR call it in your code!
     *
     * @noinspection ALL
     */
    private void $$$setupUI$$$() {
        panel1 = new JPanel();
        panel1.setLayout(new BorderLayout(0, 0));
        panel4 = new JPanel();
        panel4.setLayout(new BorderLayout(0, 0));
        panel1.add(panel4, BorderLayout.NORTH);
        panel2 = new JPanel();
        panel2.setLayout(new BorderLayout(0, 0));
        panel4.add(panel2, BorderLayout.WEST);
        final JLabel label1 = new JLabel();
        label1.setText("Segment:");
        panel2.add(label1, BorderLayout.WEST);
        textField1 = new JTextField();
        textField1.setEditable(false);
        textField1.setEnabled(false);
        panel2.add(textField1, BorderLayout.EAST);
        panel3 = new JPanel();
        panel3.setLayout(new BorderLayout(0, 0));
        panel4.add(panel3, BorderLayout.CENTER);
        final JLabel label2 = new JLabel();
        label2.setText("Date:");
        panel3.add(label2, BorderLayout.WEST);
        textField2 = new JTextField();
        textField2.setEditable(false);
        textField2.setEnabled(false);
        panel3.add(textField2, BorderLayout.CENTER);
        final JScrollPane scrollPane1 = new JScrollPane();
        panel1.add(scrollPane1, BorderLayout.CENTER);
        table1 = new JTable();
        scrollPane1.setViewportView(table1);
    }

    /**
     * @noinspection ALL
     */
    public JComponent $$$getRootComponent$$$() {
        return panel1;
    }

    private static MainPanel refClassObject;

    public blockage_analysis(MainPanel refClassObject) {
        this.refClassObject = refClassObject;
    }

    public void setReferenceClass(MainPanel refClassObject) {
        this.refClassObject = refClassObject;
    }

    public MainPanel getReferenceClass() {
        return refClassObject;
    }

}