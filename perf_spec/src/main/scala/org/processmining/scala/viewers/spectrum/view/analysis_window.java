package org.processmining.scala.viewers.spectrum.view;

import org.processmining.scala.viewers.spectrum.SortedComboBoxModel;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableRowSorter;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

public class analysis_window extends JFrame {
    private JPanel panel1;
    private JPanel panel2;
    private JTable table1;
    private JComboBox comboBox1;
    private JComboBox comboBox2;
    private JPanel panel3;
    private JPanel panel4;
    private JPanel panel5;
    private     JTextField textField1;
    private JButton filterButton;
    private JComboBox comboBox3;
    private JScrollPane scrollPane1;
    static Double value;
    static Double value2;
    static String value_date;
    static String segment;
    static List<String> listDistinct;
    static List<String> listDistinct_cluster;
    static List<RowFilter<Object, Object>> filters;
    static List<RowFilter<Object, Object>> filters2;
    static List<RowFilter<Object, Object>> filters3;
    private static TableRowSorter<DefaultTableModel> sorter;
    private static DefaultTableModel m;

    private void createUIComponents() {
//        comboBox1.addItem(0);
//        comboBox1.addItem(0.2);
//        comboBox1.addItem(0.33);
//        comboBox1.addItem(0.5);
//        comboBox1.addItem(0.6);
//        comboBox1.addItem(0.67);
//       // comboBox2.addItem(0);
//        comboBox3.addItem(0.2);
//        comboBox3.addItem(0.33);
//        comboBox3.addItem(0.5);
//        comboBox3.addItem(0.6);
//        comboBox3.addItem(0.67);

        comboBox1.setEditable(true);
        for (int i = 0; i < listDistinct.size(); i++)
            comboBox2.addItem(listDistinct.get(i));

        for (int i = 0; i < listDistinct_cluster.size(); i++)
            comboBox1.addItem(listDistinct_cluster.get(i));

        for (int i = 1; i < listDistinct_cluster.size(); i++)
            comboBox3.addItem(listDistinct_cluster.get(i));


        this.textField1.setEditable(true);
        this.textField1.setFocusable(true);
        textField1.setSize(new Dimension(100, 24));
        filterButton.setSize(new Dimension(70, 24));
        textField1.setPreferredSize(new Dimension(100, 24));

        // TODO: place custom component creation code here
    }


    public analysis_window() throws FileNotFoundException {

        $$$setupUI$$$();
        this.panel2 = new JPanel();
        this.panel1 = new JPanel();
        this.panel3 = new JPanel();
        this.panel4 = new JPanel();
        this.panel5 = new JPanel();

        this.comboBox1 = new JComboBox();
        this.comboBox2 = new JComboBox();
        this.comboBox3 = new JComboBox();

        this.textField1 = new JTextField();
        this.filterButton = new JButton("Filter");


        this.sorter = new TableRowSorter<DefaultTableModel>();
        things();
        createUIComponents();

        comboBox2.setSelectedIndex(-1);
        comboBox2.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JComboBox cb = (JComboBox) e.getSource();
                analysis_window.value_date = (String) cb.getSelectedItem();
                filters = new ArrayList<RowFilter<Object, Object>>(1);
                filters.add(RowFilter.regexFilter("(?i)" + value_date, 15));
                RowFilter<Object, Object> combined_filter = RowFilter.andFilter(filters);
                sorter.setRowFilter(combined_filter);

            }
        });
        comboBox2.setSelectedIndex(0);


        comboBox1.setSelectedIndex(-1);
        comboBox1.setEditable(false);
        this.value2 = Double.parseDouble(String.valueOf(comboBox3.getSelectedItem()));
        comboBox1.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                JComboBox cb = (JComboBox) e.getSource();
                analysis_window.value = Double.parseDouble(String.valueOf(cb.getSelectedItem()));
                filters = new ArrayList<RowFilter<Object, Object>>(3);
                filters2 = new ArrayList<RowFilter<Object, Object>>(2);
                filters3 = new ArrayList<RowFilter<Object, Object>>(2);
                filters2.add(RowFilter.numberFilter(RowFilter.ComparisonType.AFTER, value, 1));
                filters2.add(RowFilter.numberFilter(RowFilter.ComparisonType.EQUAL, value, 1));
                RowFilter<Object, Object> combined_filter2 = RowFilter.orFilter(filters2);

                filters3.add(RowFilter.numberFilter(RowFilter.ComparisonType.BEFORE, value2, 1));
                filters3.add(RowFilter.numberFilter(RowFilter.ComparisonType.EQUAL, value2, 1));
                RowFilter<Object, Object> combined_filter3 = RowFilter.orFilter(filters3);

                filters.add(RowFilter.regexFilter("(?i)" + value_date, 15));
                filters.add(combined_filter2);
                filters.add(combined_filter3);
                RowFilter<Object, Object> combined_filter = RowFilter.andFilter(filters);


                sorter.setRowFilter(combined_filter);
            }
        });
        comboBox1.setSelectedIndex(0);

        comboBox3.setSelectedIndex(-1);
        comboBox3.setEditable(false);
        comboBox3.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JComboBox cb = (JComboBox) e.getSource();
                analysis_window.value2 = Double.parseDouble(String.valueOf(cb.getSelectedItem()));
                filters = new ArrayList<RowFilter<Object, Object>>(3);
                filters2 = new ArrayList<RowFilter<Object, Object>>(2);
                filters2.add(RowFilter.numberFilter(RowFilter.ComparisonType.BEFORE, value2, 1));
                filters2.add(RowFilter.numberFilter(RowFilter.ComparisonType.EQUAL, value2, 1));
                RowFilter<Object, Object> combined_filter2 = RowFilter.orFilter(filters2);

                filters3.add(RowFilter.numberFilter(RowFilter.ComparisonType.AFTER, value, 1));
                filters3.add(RowFilter.numberFilter(RowFilter.ComparisonType.EQUAL, value, 1));
                RowFilter<Object, Object> combined_filter3 = RowFilter.orFilter(filters3);

                filters.add(RowFilter.regexFilter("(?i)" + value_date, 15));
                filters.add(combined_filter2);
                filters.add(combined_filter3);
                RowFilter<Object, Object> combined_filter = RowFilter.andFilter(filters);

                sorter.setRowFilter(combined_filter);
            }
        });
        comboBox3.setSelectedIndex(0);

        table1.addMouseListener(new MouseAdapter() {
        });
        filterButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                segment = textField1.getText();
                filters = new ArrayList<RowFilter<Object, Object>>(2);
                filters.add(RowFilter.regexFilter("(?i)" + value_date, 15));
                filters.add(RowFilter.regexFilter("(?i)" + segment, 0));
                RowFilter<Object, Object> combined_filter = RowFilter.andFilter(filters);
                sorter.setRowFilter(combined_filter);

            }
        });

    }

    //    RowFilter<DefaultTableModel, Integer> GreaterThan = new RowFilter<DefaultTableModel, Integer>() {
//        public boolean include(Entry<? extends DefaultTableModel, ? extends Integer> entry) {
//            //m = entry.getModel();
//            Integer b = entry.getIdentifier();
//            Object k = table1.getValueAt(entry.getIdentifier() + 1, 1);
//            final boolean b1 = Double.parseDouble(String.valueOf(k)) > 0.1;
//            if (Double.parseDouble(String.valueOf(table1.getValueAt(entry.getIdentifier() + 1, 1))) > value) {
//                return true;
//            }
//            return false;
//        }
//
//    };
//
//    RowFilter<DefaultTableModel, Integer> LessThan = new RowFilter<DefaultTableModel, Integer>() {
//        public boolean include(Entry<? extends DefaultTableModel, ? extends Integer> entry) {
//            // m = entry.getModel();
//            //0 is the first column
//            if (Double.parseDouble(String.valueOf(table1.getValueAt(entry.getIdentifier() + 1, 1))) < value2) {
//                return true;
//            }
//            return false;
//        }
//    };
    public void things() {
        try {
            String datafile = "C:\\Users\\nlokor\\Desktop\\Outlier_Detection\\analysis_psm_general\\Friday.csv";
            FileReader fin = new FileReader(datafile);
            m = createTableModel(fin, null);


            ArrayList<String> list = new ArrayList<String>();
            ArrayList<String> cluster_name_list = new ArrayList<String>();
            BufferedReader br = new BufferedReader(new FileReader(datafile));
            br.readLine(); // this will read the first line
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] s = line.split(",");
                list.add(s[15]);
                cluster_name_list.add(s[1]);
            }
            br.close();
            analysis_window.listDistinct = list.stream().distinct().collect(Collectors.toList());
            analysis_window.listDistinct_cluster = cluster_name_list.stream().distinct().sorted(String::compareTo).collect(Collectors.toList());

            JFrame f = new JFrame();
            f.setLocationRelativeTo(null);
            //f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);


            this.table1 = new JTable(m);
            this.sorter = new TableRowSorter<DefaultTableModel>(m);


            this.panel1.add(this.table1);
            this.panel1.add(this.panel2);

            this.table1.setRowSorter(sorter);
            this.table1.addMouseListener(new MouseAdapter() {
                @Override
                public void mouseReleased(MouseEvent e) {
                    JTable table = (JTable) e.getSource();
                    int row = table.getSelectedRow();
                    //int row = table.rowAtPoint(new Point(0, e.getY() - table.getLocation().y)) - 2;
                    String segmentName = table.getValueAt(row, 0) + "";
                    String date = table.getValueAt(row, 15) + "";
                    try {
                        blockage_analysis newFrame = new blockage_analysis(segmentName, date);
                        newFrame.setReferenceClass(refClassObject);
                        newFrame.main(new String[]{});
                    } catch (FileNotFoundException ex) {
                        ex.printStackTrace();
                    }
                }
            });


            DecimalFormat df = new DecimalFormat("####0.00");
            df.setGroupingUsed(false);
            for (int i = 1; i < this.table1.getRowCount(); i++) {

                Object avg_outlier_score = this.table1.getModel().getValueAt(i, 13);
                String avg_outlier_score_value = String.valueOf(avg_outlier_score).trim().replaceAll("\"", "");
                Double new_value_avg_outlier_score = Double.parseDouble(df.format(Double.valueOf(avg_outlier_score_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_avg_outlier_score, i, 13);

                Object total_blockage_time = this.table1.getModel().getValueAt(i, 7);
                String total_blockage_time_value = String.valueOf(total_blockage_time).trim().replaceAll("\"", "");
                Double new_value_total_blockage_time = Double.parseDouble(df.format(Double.valueOf(total_blockage_time_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_total_blockage_time, i, 7);

                Object avg_blockage_time = this.table1.getModel().getValueAt(i, 8);
                String avg_blockage_time_value = String.valueOf(avg_blockage_time).trim().replaceAll("\"", "");
                Double new_value_avg_blockage_time = Double.parseDouble(df.format(Double.valueOf(avg_blockage_time_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_avg_blockage_time, i, 8);

                Object avg_duration_bags = this.table1.getModel().getValueAt(i, 5);
                String avg_duration_bags_value = String.valueOf(avg_duration_bags).trim().replaceAll("\"", "");
                Double new_value_avg_duration_bags = Double.parseDouble(df.format(Double.valueOf(avg_duration_bags_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_avg_duration_bags, i, 5);

                Object max_score = this.table1.getModel().getValueAt(i, 4);
                String max_score_value = String.valueOf(max_score).trim().replaceAll("\"", "");
                Double new_value_max_score = Double.parseDouble(df.format(Double.valueOf(max_score_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_max_score, i, 4);

                Object min_score = this.table1.getModel().getValueAt(i, 3);
                String min_score_value = String.valueOf(min_score).trim().replaceAll("\"", "");
                Double new_value_min_score = Double.parseDouble(df.format(Double.valueOf(min_score_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_min_score, i, 3);

                Object cluster_mean = this.table1.getModel().getValueAt(i, 2);
                String cluster_mean_value = String.valueOf(cluster_mean).trim().replaceAll("\"", "");
                Double new_value_cluster_mean = Double.parseDouble(df.format(Double.valueOf(cluster_mean_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_cluster_mean, i, 2);

                Object cluster = this.table1.getModel().getValueAt(i, 1);
                String cluster_value = String.valueOf(cluster).trim().replaceAll("\"", "");
                Double new_value_cluster = Double.parseDouble(df.format(Double.valueOf(cluster_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_cluster, i, 1);

                Object importance = this.table1.getModel().getValueAt(i, 14);
                String importance_value = String.valueOf(importance).trim().replaceAll("\"", "");
                Double new_value_importance = Double.parseDouble(df.format(Double.valueOf(importance_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_importance, i, 14);

                Object bags_in_blockage = this.table1.getModel().getValueAt(i, 6);
                String bags_in_blockage_value = String.valueOf(bags_in_blockage).trim().replaceAll("\"", "");
                Double new_value_bags_in_blockage = Double.parseDouble(df.format(Double.parseDouble(bags_in_blockage_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_bags_in_blockage, i, 6);

                Object isolated_bag_count = this.table1.getModel().getValueAt(i, 9);
                String isolated_bag_count_value = String.valueOf(isolated_bag_count).trim().replaceAll("\"", "");
                Double new_value_isolated_bag_count = Double.parseDouble(df.format(Double.parseDouble(isolated_bag_count_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_isolated_bag_count, i, 9);

                Object fast_bag_count = this.table1.getModel().getValueAt(i, 10);
                String fast_bag_count_value = String.valueOf(fast_bag_count).trim().replaceAll("\"", "");
                Double new_value_fast_bag_count = Double.parseDouble(df.format(Double.parseDouble(fast_bag_count_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_fast_bag_count, i, 10);

                Object number_of_outliers = this.table1.getModel().getValueAt(i, 11);
                String number_of_outliers_value = String.valueOf(number_of_outliers).trim().replaceAll("\"", "");
                Double new_value_number_of_outliers = Double.parseDouble(df.format(Double.parseDouble(number_of_outliers_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_number_of_outliers, i, 11);

                Object total_bag = this.table1.getModel().getValueAt(i, 12);
                String total_bag_value = String.valueOf(total_bag).trim().replaceAll("\"", "");
                Double new_value_total_bag = Double.parseDouble(df.format(Double.parseDouble(total_bag_value)).replace(',', '.'));
                this.table1.getModel().setValueAt(new_value_total_bag, i, 12);

            }
            this.filters = new ArrayList<RowFilter<Object, Object>>(1);
            filters.add(RowFilter.regexFilter("(?i)" + value_date, 15));
            //filters.add(RowFilter.numberFilter(RowFilter.ComparisonType.AFTER, this.value, 1));
            //filters.add(RowFilter.numberFilter(RowFilter.ComparisonType.BEFORE, this.value2, 1));
            //sorter.setRowFilter(GreaterThan);
            //sorter.setRowFilter(LessThan);
            RowFilter<Object, Object> combined_filter = RowFilter.andFilter(filters);

            this.sorter.setRowFilter(combined_filter);
            this.table1.getTableHeader().setFont(new Font("Arial", Font.BOLD, 12));
            this.table1.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);

            f.add(panel1);
            this.panel1.add(new JScrollPane(this.table1));
            this.panel1.add(this.panel2);

            this.panel2.add(this.panel4);
            this.panel2.add(this.panel3);
            this.panel2.add(this.panel5);
            this.panel4.add(this.comboBox1);
            this.panel4.add(this.comboBox3);
            this.panel3.add(this.comboBox2);
            this.panel5.add(this.textField1);
            this.panel5.add(this.filterButton);


            f.getContentPane().add(new JScrollPane(this.table1), BorderLayout.CENTER);
            f.getContentPane().add(this.panel2, BorderLayout.NORTH);


            f.setExtendedState(JFrame.MAXIMIZED_BOTH);
            f.setVisible(true);
            pack();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * @param in      A CSV input stream to parse
     * @param headers A Vector containing the column headers. If this is null, it's assumed
     *                that the first row contains column headers
     * @return A DefaultTableModel containing the CSV values as type String
     */
    public static DefaultTableModel createTableModel(Reader in,
                                                     Vector<Object> headers) {

        DefaultTableModel model = null;
        Scanner s = null;

        try {
            Vector<Vector<Object>> rows = new Vector<Vector<Object>>();
            s = new Scanner(in);
            ArrayList<Double> avg_outlier_score_list = new ArrayList<Double>();
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
                                return Double.class;
                            case 2:
                                return Double.class;
                            case 3:
                                return Double.class;
                            case 4:
                                return Double.class;
                            case 5:
                                return Double.class;
                            case 6:
                                return Double.class;
                            case 7:
                                return Double.class;
                            case 8:
                                return Double.class;
                            case 9:
                                return Double.class;
                            case 10:
                                return Double.class;
                            case 11:
                                return Double.class;
                            case 12:
                                return Double.class;
                            case 13:
                                return Double.class;
                            case 14:
                                return Double.class;
                            case 15:
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
                                return Double.class;
                            case 2:
                                return Double.class;
                            case 3:
                                return Double.class;
                            case 4:
                                return Double.class;
                            case 5:
                                return Double.class;
                            case 6:
                                return Double.class;
                            case 7:
                                return Double.class;
                            case 8:
                                return Double.class;
                            case 9:
                                return Double.class;
                            case 10:
                                return Double.class;
                            case 11:
                                return Double.class;
                            case 12:
                                return Double.class;
                            case 13:
                                return Double.class;
                            case 14:
                                return Double.class;
                            case 15:
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
        final JScrollPane scrollPane2 = new JScrollPane();
        scrollPane2.setMaximumSize(new Dimension(32767, 150));
        panel1.add(scrollPane2, BorderLayout.CENTER);
        table1 = new JTable();
        scrollPane2.setViewportView(table1);
        panel2 = new JPanel();
        panel2.setLayout(new BorderLayout(0, 0));
        panel1.add(panel2, BorderLayout.NORTH);
        panel3 = new JPanel();
        panel3.setLayout(new BorderLayout(0, 0));
        panel2.add(panel3, BorderLayout.WEST);
        comboBox2 = new JComboBox();
        comboBox2.setEditable(false);
        comboBox2.setEnabled(true);
        final DefaultComboBoxModel defaultComboBoxModel1 = new DefaultComboBoxModel();
        comboBox2.setModel(defaultComboBoxModel1);
        comboBox2.setPreferredSize(new Dimension(110, 24));
        comboBox2.setRequestFocusEnabled(true);
        panel3.add(comboBox2, BorderLayout.CENTER);
        final JLabel label1 = new JLabel();
        label1.setText("Date:");
        panel3.add(label1, BorderLayout.WEST);
        panel4 = new JPanel();
        panel4.setLayout(new BorderLayout(0, 0));
        panel2.add(panel4, BorderLayout.CENTER);
        comboBox1 = new JComboBox();
        comboBox1.setEditable(false);
        comboBox1.setMaximumSize(new Dimension(32767, 100));
        final DefaultComboBoxModel defaultComboBoxModel2 = new DefaultComboBoxModel();
        defaultComboBoxModel2.addElement("0");
        defaultComboBoxModel2.addElement("0,2");
        defaultComboBoxModel2.addElement("0,25");
        defaultComboBoxModel2.addElement("0,33");
        defaultComboBoxModel2.addElement("0,5");
        defaultComboBoxModel2.addElement("0,6");
        defaultComboBoxModel2.addElement("0,67");
        defaultComboBoxModel2.addElement("0,75");
        defaultComboBoxModel2.addElement("0,83");
        defaultComboBoxModel2.addElement("1");
        comboBox1.setModel(defaultComboBoxModel2);
        comboBox1.setPreferredSize(new Dimension(110, 24));
        comboBox1.setRequestFocusEnabled(true);
        panel4.add(comboBox1, BorderLayout.NORTH);
        comboBox3 = new JComboBox();
        final DefaultComboBoxModel defaultComboBoxModel3 = new DefaultComboBoxModel();
        defaultComboBoxModel3.addElement("0");
        defaultComboBoxModel3.addElement("0,2");
        defaultComboBoxModel3.addElement("0,25");
        defaultComboBoxModel3.addElement("0,33");
        defaultComboBoxModel3.addElement("0,5");
        defaultComboBoxModel3.addElement("0,6");
        defaultComboBoxModel3.addElement("0,67");
        defaultComboBoxModel3.addElement("0,75");
        defaultComboBoxModel3.addElement("0,83");
        defaultComboBoxModel3.addElement("1");
        comboBox3.setModel(defaultComboBoxModel3);
        panel4.add(comboBox3, BorderLayout.CENTER);
        panel5 = new JPanel();
        panel5.setLayout(new BorderLayout(0, 0));
        panel5.setMinimumSize(new Dimension(200, 24));
        panel5.setPreferredSize(new Dimension(200, 24));
        panel5.setRequestFocusEnabled(false);
        panel5.setVerifyInputWhenFocusTarget(false);
        panel2.add(panel5, BorderLayout.EAST);
        textField1 = new JTextField();
        textField1.setText("Type Segment Name");
        panel5.add(textField1, BorderLayout.CENTER);
        filterButton = new JButton();
        filterButton.setText("Filter");
        panel5.add(filterButton, BorderLayout.EAST);
        final JLabel label2 = new JLabel();
        label2.setText("Segment Name:");
        panel5.add(label2, BorderLayout.WEST);
    }

    /**
     * @noinspection ALL
     */
    public JComponent $$$getRootComponent$$$() {
        return panel1;
    }


    public class ComboItem {
        private String key;
        private String value;

        public ComboItem(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return key;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }

    private static MainPanel refClassObject;

    public analysis_window(MainPanel refClassObject) {
        this.refClassObject = refClassObject;
    }

    public void setReferenceClass(MainPanel refClassObject) {
        this.refClassObject = refClassObject;
    }

    public MainPanel getReferenceClass() {
        return refClassObject;
    }
}
