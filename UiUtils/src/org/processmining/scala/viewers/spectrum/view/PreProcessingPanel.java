/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.processmining.scala.viewers.spectrum.view;

/**
 *
 * @author nlvden
 */
public class PreProcessingPanel extends javax.swing.JPanel {

    /**
     * Creates new form PreProcessingPanel
     */
    public PreProcessingPanel() {
        initComponents();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jPanel1 = new javax.swing.JPanel();
        jButtonRun = new javax.swing.JButton();
        jPanel21 = new javax.swing.JPanel();
        jButtonCancel = new javax.swing.JButton();
        jPanel22 = new javax.swing.JPanel();
        jPanel23 = new javax.swing.JPanel();
        jPanel24 = new javax.swing.JPanel();
        jButtonRunAndOpen = new javax.swing.JButton();
        jPanel25 = new javax.swing.JPanel();
        jPanel26 = new javax.swing.JPanel();
        jPanel27 = new javax.swing.JPanel();
        jButtonHelp = new javax.swing.JButton();
        jPanel2 = new javax.swing.JPanel();
        jPanelInput = new javax.swing.JPanel();
        jLabel6 = new javax.swing.JLabel();
        jPanel15 = new javax.swing.JPanel();
        jButtonOpenLog = new javax.swing.JButton();
        jTextFieldFileName = new javax.swing.JTextField();
        jPanel6 = new javax.swing.JPanel();
        jPanel7 = new javax.swing.JPanel();
        jLabel1 = new javax.swing.JLabel();
        jTextFieldTimeWindow = new javax.swing.JTextField();
        jPanel8 = new javax.swing.JPanel();
        jPanel9 = new javax.swing.JPanel();
        jLabel2 = new javax.swing.JLabel();
        jComboBoxAggregationFunction = new javax.swing.JComboBox<>();
        jPanel10 = new javax.swing.JPanel();
        jPanel11 = new javax.swing.JPanel();
        jLabel3 = new javax.swing.JLabel();
        jComboBoxDurationClassifier = new javax.swing.JComboBox<>();
        jPanel12 = new javax.swing.JPanel();
        jPanel13 = new javax.swing.JPanel();
        jLabel4 = new javax.swing.JLabel();
        jTextFieldActivityClassifier = new javax.swing.JTextField();
        jPanel4 = new javax.swing.JPanel();
        jPanel14 = new javax.swing.JPanel();
        jPanel5 = new javax.swing.JPanel();
        jPanelInput1 = new javax.swing.JPanel();
        jLabel7 = new javax.swing.JLabel();
        jPanel16 = new javax.swing.JPanel();
        jButtonOpenLog1 = new javax.swing.JButton();
        jTextFieldOutDir = new javax.swing.JTextField();
        jPanel3 = new javax.swing.JPanel();
        jPanel17 = new javax.swing.JPanel();
        jLabel8 = new javax.swing.JLabel();
        jLabelXesLogImportProgress = new javax.swing.JLabel();
        jPanel18 = new javax.swing.JPanel();
        jPanel19 = new javax.swing.JPanel();
        jLabel10 = new javax.swing.JLabel();
        jLabelPreProcessingProgress = new javax.swing.JLabel();
        jPanel20 = new javax.swing.JPanel();
        jLabelMsg = new javax.swing.JLabel();

        setBorder(javax.swing.BorderFactory.createEmptyBorder(10, 10, 10, 10));
        setMinimumSize(new java.awt.Dimension(700, 375));
        setLayout(new java.awt.BorderLayout());

        jPanel1.setPreferredSize(new java.awt.Dimension(0, 32));
        jPanel1.setLayout(new java.awt.BorderLayout());

        jButtonRun.setText("Process");
        jButtonRun.setPreferredSize(new java.awt.Dimension(100, 25));
        jButtonRun.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonRunActionPerformed(evt);
            }
        });
        jPanel1.add(jButtonRun, java.awt.BorderLayout.LINE_END);

        jPanel21.setLayout(new java.awt.BorderLayout());

        jButtonCancel.setText("Cancel");
        jButtonCancel.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonCancelActionPerformed(evt);
            }
        });
        jPanel21.add(jButtonCancel, java.awt.BorderLayout.LINE_START);

        jPanel22.setLayout(new java.awt.BorderLayout());

        jPanel23.setPreferredSize(new java.awt.Dimension(15, 0));
        jPanel23.setLayout(new java.awt.BorderLayout());
        jPanel22.add(jPanel23, java.awt.BorderLayout.LINE_END);

        jPanel24.setLayout(new java.awt.BorderLayout());

        jButtonRunAndOpen.setText("Process & open");
        jButtonRunAndOpen.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonRunAndOpenActionPerformed(evt);
            }
        });
        jPanel24.add(jButtonRunAndOpen, java.awt.BorderLayout.LINE_END);

        jPanel25.setLayout(new java.awt.BorderLayout());

        jPanel26.setPreferredSize(new java.awt.Dimension(10, 0));

        javax.swing.GroupLayout jPanel26Layout = new javax.swing.GroupLayout(jPanel26);
        jPanel26.setLayout(jPanel26Layout);
        jPanel26Layout.setHorizontalGroup(
            jPanel26Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 10, Short.MAX_VALUE)
        );
        jPanel26Layout.setVerticalGroup(
            jPanel26Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 32, Short.MAX_VALUE)
        );

        jPanel25.add(jPanel26, java.awt.BorderLayout.LINE_START);

        jPanel27.setLayout(new java.awt.BorderLayout());

        jButtonHelp.setText("Help");
        jButtonHelp.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonHelpActionPerformed(evt);
            }
        });
        jPanel27.add(jButtonHelp, java.awt.BorderLayout.LINE_START);

        jPanel25.add(jPanel27, java.awt.BorderLayout.CENTER);

        jPanel24.add(jPanel25, java.awt.BorderLayout.CENTER);

        jPanel22.add(jPanel24, java.awt.BorderLayout.CENTER);

        jPanel21.add(jPanel22, java.awt.BorderLayout.CENTER);

        jPanel1.add(jPanel21, java.awt.BorderLayout.CENTER);

        add(jPanel1, java.awt.BorderLayout.SOUTH);

        jPanel2.setLayout(new java.awt.BorderLayout());

        jPanelInput.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        jPanelInput.setPreferredSize(new java.awt.Dimension(640, 43));
        jPanelInput.setLayout(new java.awt.BorderLayout());

        jLabel6.setText("Event log:");
        jLabel6.setPreferredSize(new java.awt.Dimension(220, 0));
        jPanelInput.add(jLabel6, java.awt.BorderLayout.LINE_START);

        jPanel15.setLayout(new java.awt.BorderLayout());

        jButtonOpenLog.setText("Open...");
        jButtonOpenLog.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonOpenLogActionPerformed(evt);
            }
        });
        jPanel15.add(jButtonOpenLog, java.awt.BorderLayout.LINE_END);
        jPanel15.add(jTextFieldFileName, java.awt.BorderLayout.CENTER);

        jPanelInput.add(jPanel15, java.awt.BorderLayout.CENTER);

        jPanel2.add(jPanelInput, java.awt.BorderLayout.NORTH);

        jPanel6.setLayout(new java.awt.BorderLayout());

        jPanel7.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        jPanel7.setPreferredSize(new java.awt.Dimension(640, 43));
        jPanel7.setLayout(new java.awt.BorderLayout());

        jLabel1.setText("Bin size (e.g. 1d 2h 5m 10s 100ms):");
        jLabel1.setPreferredSize(new java.awt.Dimension(220, 0));
        jPanel7.add(jLabel1, java.awt.BorderLayout.LINE_START);
        jPanel7.add(jTextFieldTimeWindow, java.awt.BorderLayout.CENTER);

        jPanel6.add(jPanel7, java.awt.BorderLayout.NORTH);

        jPanel8.setLayout(new java.awt.BorderLayout());

        jPanel9.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        jPanel9.setPreferredSize(new java.awt.Dimension(640, 43));
        jPanel9.setLayout(new java.awt.BorderLayout());

        jLabel2.setText("Aggregation function:");
        jLabel2.setPreferredSize(new java.awt.Dimension(220, 0));
        jPanel9.add(jLabel2, java.awt.BorderLayout.LINE_START);

        jComboBoxAggregationFunction.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "Cases pending", "Cases started", "Cases stopped" }));
        jComboBoxAggregationFunction.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jComboBoxAggregationFunctionActionPerformed(evt);
            }
        });
        jPanel9.add(jComboBoxAggregationFunction, java.awt.BorderLayout.CENTER);

        jPanel8.add(jPanel9, java.awt.BorderLayout.NORTH);

        jPanel10.setLayout(new java.awt.BorderLayout());

        jPanel11.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        jPanel11.setPreferredSize(new java.awt.Dimension(640, 43));
        jPanel11.setLayout(new java.awt.BorderLayout());

        jLabel3.setText("Duration classifier:");
        jLabel3.setPreferredSize(new java.awt.Dimension(220, 0));
        jPanel11.add(jLabel3, java.awt.BorderLayout.LINE_START);

        jComboBoxDurationClassifier.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "Quartile-based", "Median-proportional" }));
        jPanel11.add(jComboBoxDurationClassifier, java.awt.BorderLayout.CENTER);

        jPanel10.add(jPanel11, java.awt.BorderLayout.NORTH);

        jPanel12.setLayout(new java.awt.BorderLayout());

        jPanel13.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        jPanel13.setPreferredSize(new java.awt.Dimension(640, 43));
        jPanel13.setLayout(new java.awt.BorderLayout());

        jLabel4.setText("Activity classifier (opt., e.g. a1;a2):");
        jLabel4.setPreferredSize(new java.awt.Dimension(220, 0));
        jPanel13.add(jLabel4, java.awt.BorderLayout.LINE_START);

        jTextFieldActivityClassifier.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jTextFieldActivityClassifierActionPerformed(evt);
            }
        });
        jPanel13.add(jTextFieldActivityClassifier, java.awt.BorderLayout.CENTER);

        jPanel12.add(jPanel13, java.awt.BorderLayout.NORTH);

        jPanel4.setLayout(new java.awt.BorderLayout());

        jPanel14.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        jPanel14.setPreferredSize(new java.awt.Dimension(640, 0));
        jPanel14.setLayout(new java.awt.BorderLayout());
        jPanel4.add(jPanel14, java.awt.BorderLayout.NORTH);

        jPanel5.setLayout(new java.awt.BorderLayout());

        jPanelInput1.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        jPanelInput1.setPreferredSize(new java.awt.Dimension(640, 43));
        jPanelInput1.setLayout(new java.awt.BorderLayout());

        jLabel7.setText("Output directory:");
        jLabel7.setPreferredSize(new java.awt.Dimension(220, 0));
        jPanelInput1.add(jLabel7, java.awt.BorderLayout.LINE_START);

        jPanel16.setLayout(new java.awt.BorderLayout());

        jButtonOpenLog1.setText("Open...");
        jButtonOpenLog1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonOpenLog1ActionPerformed(evt);
            }
        });
        jPanel16.add(jButtonOpenLog1, java.awt.BorderLayout.LINE_END);
        jPanel16.add(jTextFieldOutDir, java.awt.BorderLayout.CENTER);

        jPanelInput1.add(jPanel16, java.awt.BorderLayout.CENTER);

        jPanel5.add(jPanelInput1, java.awt.BorderLayout.NORTH);

        jPanel3.setLayout(new java.awt.BorderLayout());

        jPanel17.setBorder(javax.swing.BorderFactory.createEmptyBorder(25, 5, 5, 5));
        jPanel17.setPreferredSize(new java.awt.Dimension(640, 63));
        jPanel17.setLayout(new java.awt.BorderLayout());

        jLabel8.setText("XES Log import:");
        jLabel8.setPreferredSize(new java.awt.Dimension(220, 0));
        jPanel17.add(jLabel8, java.awt.BorderLayout.LINE_START);

        jLabelXesLogImportProgress.setText("(not started)");
        jPanel17.add(jLabelXesLogImportProgress, java.awt.BorderLayout.CENTER);

        jPanel3.add(jPanel17, java.awt.BorderLayout.NORTH);

        jPanel18.setLayout(new java.awt.BorderLayout());

        jPanel19.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        jPanel19.setPreferredSize(new java.awt.Dimension(640, 43));
        jPanel19.setLayout(new java.awt.BorderLayout());

        jLabel10.setText("Log pre-processing:");
        jLabel10.setPreferredSize(new java.awt.Dimension(220, 0));
        jPanel19.add(jLabel10, java.awt.BorderLayout.LINE_START);

        jLabelPreProcessingProgress.setText("(not started)");
        jPanel19.add(jLabelPreProcessingProgress, java.awt.BorderLayout.CENTER);

        jPanel18.add(jPanel19, java.awt.BorderLayout.NORTH);

        jPanel20.setLayout(new java.awt.BorderLayout());
        jPanel20.add(jLabelMsg, java.awt.BorderLayout.PAGE_START);

        jPanel18.add(jPanel20, java.awt.BorderLayout.CENTER);

        jPanel3.add(jPanel18, java.awt.BorderLayout.CENTER);

        jPanel5.add(jPanel3, java.awt.BorderLayout.CENTER);

        jPanel4.add(jPanel5, java.awt.BorderLayout.CENTER);

        jPanel12.add(jPanel4, java.awt.BorderLayout.CENTER);

        jPanel10.add(jPanel12, java.awt.BorderLayout.CENTER);

        jPanel8.add(jPanel10, java.awt.BorderLayout.CENTER);

        jPanel6.add(jPanel8, java.awt.BorderLayout.CENTER);

        jPanel2.add(jPanel6, java.awt.BorderLayout.CENTER);

        add(jPanel2, java.awt.BorderLayout.CENTER);
    }// </editor-fold>//GEN-END:initComponents

    private void jTextFieldActivityClassifierActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jTextFieldActivityClassifierActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jTextFieldActivityClassifierActionPerformed

    private void jButtonOpenLogActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonOpenLogActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jButtonOpenLogActionPerformed

    private void jComboBoxAggregationFunctionActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jComboBoxAggregationFunctionActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jComboBoxAggregationFunctionActionPerformed

    private void jButtonOpenLog1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonOpenLog1ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jButtonOpenLog1ActionPerformed

    private void jButtonCancelActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonCancelActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jButtonCancelActionPerformed

    private void jButtonRunActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonRunActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jButtonRunActionPerformed

    private void jButtonRunAndOpenActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonRunAndOpenActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jButtonRunAndOpenActionPerformed

    private void jButtonHelpActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonHelpActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jButtonHelpActionPerformed


    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton jButtonCancel;
    private javax.swing.JButton jButtonHelp;
    private javax.swing.JButton jButtonOpenLog;
    private javax.swing.JButton jButtonOpenLog1;
    private javax.swing.JButton jButtonRun;
    private javax.swing.JButton jButtonRunAndOpen;
    private javax.swing.JComboBox<String> jComboBoxAggregationFunction;
    private javax.swing.JComboBox<String> jComboBoxDurationClassifier;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel10;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabel4;
    private javax.swing.JLabel jLabel6;
    private javax.swing.JLabel jLabel7;
    private javax.swing.JLabel jLabel8;
    private javax.swing.JLabel jLabelMsg;
    private javax.swing.JLabel jLabelPreProcessingProgress;
    private javax.swing.JLabel jLabelXesLogImportProgress;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel10;
    private javax.swing.JPanel jPanel11;
    private javax.swing.JPanel jPanel12;
    private javax.swing.JPanel jPanel13;
    private javax.swing.JPanel jPanel14;
    private javax.swing.JPanel jPanel15;
    private javax.swing.JPanel jPanel16;
    private javax.swing.JPanel jPanel17;
    private javax.swing.JPanel jPanel18;
    private javax.swing.JPanel jPanel19;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JPanel jPanel20;
    private javax.swing.JPanel jPanel21;
    private javax.swing.JPanel jPanel22;
    private javax.swing.JPanel jPanel23;
    private javax.swing.JPanel jPanel24;
    private javax.swing.JPanel jPanel25;
    private javax.swing.JPanel jPanel26;
    private javax.swing.JPanel jPanel27;
    private javax.swing.JPanel jPanel3;
    private javax.swing.JPanel jPanel4;
    private javax.swing.JPanel jPanel5;
    private javax.swing.JPanel jPanel6;
    private javax.swing.JPanel jPanel7;
    private javax.swing.JPanel jPanel8;
    private javax.swing.JPanel jPanel9;
    private javax.swing.JPanel jPanelInput;
    private javax.swing.JPanel jPanelInput1;
    private javax.swing.JTextField jTextFieldActivityClassifier;
    private javax.swing.JTextField jTextFieldFileName;
    private javax.swing.JTextField jTextFieldOutDir;
    private javax.swing.JTextField jTextFieldTimeWindow;
    // End of variables declaration//GEN-END:variables
}