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
public class DatasetOpenDialog extends javax.swing.JDialog {

    /**
     * Creates new form DatasetOpenDialog
     */
    public DatasetOpenDialog(java.awt.Frame parent, boolean modal) {
        super(parent, modal);
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
        jPanel9 = new javax.swing.JPanel();
        jLabel2 = new javax.swing.JLabel();
        jComboBoxAggregationAny = new javax.swing.JComboBox<>();
        jPanel2 = new javax.swing.JPanel();
        jPanel10 = new javax.swing.JPanel();
        jLabel3 = new javax.swing.JLabel();
        jComboBoxCaching = new javax.swing.JComboBox<>();
        jPanel3 = new javax.swing.JPanel();
        jPanel11 = new javax.swing.JPanel();
        jLabel1 = new javax.swing.JLabel();
        jLabelProgress = new javax.swing.JLabel();
        jPanel4 = new javax.swing.JPanel();
        jPanel5 = new javax.swing.JPanel();
        jPanel6 = new javax.swing.JPanel();
        jButtonOpen = new javax.swing.JButton();
        jPanel21 = new javax.swing.JPanel();
        jButtonCancel = new javax.swing.JButton();
        jPanel22 = new javax.swing.JPanel();
        jPanel23 = new javax.swing.JPanel();
        jPanel24 = new javax.swing.JPanel();
        jPanel25 = new javax.swing.JPanel();
        jPanel26 = new javax.swing.JPanel();
        jPanel27 = new javax.swing.JPanel();
        jButtonHelp = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.DO_NOTHING_ON_CLOSE);
        setTitle("Open pre-processed dataset");
        setResizable(false);

        jPanel1.setBorder(javax.swing.BorderFactory.createEmptyBorder(10, 10, 10, 10));
        jPanel1.setLayout(new java.awt.BorderLayout());

        jPanel9.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        jPanel9.setPreferredSize(new java.awt.Dimension(640, 43));
        jPanel9.setLayout(new java.awt.BorderLayout());

        jLabel2.setText("Activity aggregation (before/after):");
        jLabel2.setPreferredSize(new java.awt.Dimension(220, 0));
        jPanel9.add(jLabel2, java.awt.BorderLayout.LINE_START);

        jComboBoxAggregationAny.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "None", "Any -> A", "A -> Any" }));
        jComboBoxAggregationAny.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jComboBoxAggregationAnyActionPerformed(evt);
            }
        });
        jPanel9.add(jComboBoxAggregationAny, java.awt.BorderLayout.CENTER);

        jPanel1.add(jPanel9, java.awt.BorderLayout.NORTH);

        jPanel2.setLayout(new java.awt.BorderLayout());

        jPanel10.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        jPanel10.setPreferredSize(new java.awt.Dimension(640, 43));
        jPanel10.setLayout(new java.awt.BorderLayout());

        jLabel3.setText("Caching:");
        jLabel3.setPreferredSize(new java.awt.Dimension(220, 0));
        jPanel10.add(jLabel3, java.awt.BorderLayout.LINE_START);

        jComboBoxCaching.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "Load on open", "Load on demand" }));
        jComboBoxCaching.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jComboBoxCachingActionPerformed(evt);
            }
        });
        jPanel10.add(jComboBoxCaching, java.awt.BorderLayout.CENTER);

        jPanel2.add(jPanel10, java.awt.BorderLayout.NORTH);

        jPanel3.setLayout(new java.awt.BorderLayout());

        jPanel11.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        jPanel11.setPreferredSize(new java.awt.Dimension(640, 43));
        jPanel11.setLayout(new java.awt.BorderLayout());

        jLabel1.setText("Progress:");
        jLabel1.setPreferredSize(new java.awt.Dimension(220, 0));
        jPanel11.add(jLabel1, java.awt.BorderLayout.LINE_START);

        jLabelProgress.setText("(not started)");
        jPanel11.add(jLabelProgress, java.awt.BorderLayout.CENTER);

        jPanel3.add(jPanel11, java.awt.BorderLayout.NORTH);

        jPanel4.setLayout(new java.awt.BorderLayout());

        jPanel5.setLayout(new java.awt.BorderLayout());

        jPanel6.setPreferredSize(new java.awt.Dimension(0, 32));
        jPanel6.setLayout(new java.awt.BorderLayout());

        jButtonOpen.setText("Open");
        jButtonOpen.setActionCommand("Process & open");
        jButtonOpen.setPreferredSize(new java.awt.Dimension(100, 25));
        jButtonOpen.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonOpenActionPerformed(evt);
            }
        });
        jPanel6.add(jButtonOpen, java.awt.BorderLayout.LINE_END);

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

        jPanel6.add(jPanel21, java.awt.BorderLayout.CENTER);

        jPanel5.add(jPanel6, java.awt.BorderLayout.SOUTH);

        jPanel4.add(jPanel5, java.awt.BorderLayout.CENTER);

        jPanel3.add(jPanel4, java.awt.BorderLayout.CENTER);

        jPanel2.add(jPanel3, java.awt.BorderLayout.CENTER);

        jPanel1.add(jPanel2, java.awt.BorderLayout.CENTER);

        getContentPane().add(jPanel1, java.awt.BorderLayout.CENTER);

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void jComboBoxAggregationAnyActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jComboBoxAggregationAnyActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jComboBoxAggregationAnyActionPerformed

    private void jComboBoxCachingActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jComboBoxCachingActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jComboBoxCachingActionPerformed

    private void jButtonOpenActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonOpenActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jButtonOpenActionPerformed

    private void jButtonCancelActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonCancelActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jButtonCancelActionPerformed

    private void jButtonHelpActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonHelpActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jButtonHelpActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(DatasetOpenDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(DatasetOpenDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(DatasetOpenDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(DatasetOpenDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the dialog */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                DatasetOpenDialog dialog = new DatasetOpenDialog(new javax.swing.JFrame(), true);
                dialog.addWindowListener(new java.awt.event.WindowAdapter() {
                    @Override
                    public void windowClosing(java.awt.event.WindowEvent e) {
                        System.exit(0);
                    }
                });
                dialog.setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton jButtonCancel;
    private javax.swing.JButton jButtonHelp;
    private javax.swing.JButton jButtonOpen;
    private javax.swing.JComboBox<String> jComboBoxAggregationAny;
    private javax.swing.JComboBox<String> jComboBoxCaching;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabelProgress;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel10;
    private javax.swing.JPanel jPanel11;
    private javax.swing.JPanel jPanel2;
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
    private javax.swing.JPanel jPanel9;
    // End of variables declaration//GEN-END:variables
}
