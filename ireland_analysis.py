"""
Ireland Groundwater Data Analysis Module
Exploratory Data Analysis, Geographic Analysis, and Well Characteristics Study

Improvements Applied:
1. Logging: Replaced print() with structured logging
"""

import sys
from pathlib import Path

# Add parent directory to path for shared imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

from shared.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

# Set style for all plots
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")


class IrelandGroundwaterAnalyzer:
    """Analyzer for Ireland Groundwater Wells and Springs data."""
    
    def __init__(self, df):
        self.df = df.copy()
        self.prepare_data()
    
    def prepare_data(self):
        """Prepare and clean data for analysis."""
        # Rename database columns to match analysis code
        column_mapping = {
            'hole_depth_m': 'depth_m',
            'data_completeness_score': 'completeness_score'
        }
        self.df.rename(columns=column_mapping, inplace=True)
        
        # Convert numeric columns
        numeric_cols = ['depth_m', 'yield_m3_day', 'completeness_score']
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
        
        # Create depth_category if it doesn't exist
        if 'depth_m' in self.df.columns and 'depth_category' not in self.df.columns:
            self.df['depth_category'] = pd.cut(self.df['depth_m'], 
                                               bins=[0, 30, 60, 100, 200, float('inf')],
                                               labels=['Shallow (<30m)', 'Medium (30-60m)', 
                                                      'Deep (60-100m)', 'Very Deep (100-200m)', 
                                                      'Ultra Deep (>200m)'])
        
        # Create yield_category if it doesn't exist
        if 'yield_m3_day' in self.df.columns and 'yield_category' not in self.df.columns:
            self.df['yield_category'] = pd.cut(self.df['yield_m3_day'],
                                               bins=[0, 10, 50, 100, 500, float('inf')],
                                               labels=['Very Low (<10)', 'Low (10-50)', 
                                                      'Medium (50-100)', 'High (100-500)', 
                                                      'Very High (>500)'])
    
    def get_summary_statistics(self):
        """Get summary statistics for the dataset."""
        summary = {
            'total_records': len(self.df),
            'unique_counties': self.df['county'].nunique() if 'county' in self.df.columns else 0,
            'unique_source_types': self.df['source_type'].nunique() if 'source_type' in self.df.columns else 0,
            'avg_depth_m': self.df['depth_m'].mean() if 'depth_m' in self.df.columns else None,
            'avg_yield_m3_day': self.df['yield_m3_day'].mean() if 'yield_m3_day' in self.df.columns else None,
            'avg_completeness': self.df['completeness_score'].mean() if 'completeness_score' in self.df.columns else None
        }
        
        if 'source_type' in self.df.columns:
            summary['source_types'] = self.df['source_type'].value_counts().to_dict()
        
        if 'yield_category' in self.df.columns:
            summary['yield_categories'] = self.df['yield_category'].value_counts().to_dict()
        
        return summary
    
    def plot_county_distribution(self, top_n=15, save_path=None):
        """Plot distribution of wells by county."""
        fig, axes = plt.subplots(1, 2, figsize=(16, 6))
        
        # Bar chart of top counties
        if 'county' in self.df.columns:
            county_counts = self.df['county'].value_counts().head(top_n)
            colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(county_counts)))
            
            bars = axes[0].barh(range(len(county_counts)), county_counts.values, color=colors)
            axes[0].set_yticks(range(len(county_counts)))
            axes[0].set_yticklabels(county_counts.index)
            axes[0].set_xlabel('Number of Wells', fontsize=11)
            axes[0].set_title(f'Top {top_n} Counties by Well Count', fontsize=13, fontweight='bold')
            
            # Add value labels
            for bar, val in zip(bars, county_counts.values):
                axes[0].text(val + 5, bar.get_y() + bar.get_height()/2, f'{val:,}', 
                           va='center', fontsize=9)
        
        # Pie chart of source types
        if 'source_type' in self.df.columns:
            source_counts = self.df['source_type'].value_counts()
            colors = plt.cm.Set3(np.linspace(0, 1, len(source_counts)))
            
            wedges, texts, autotexts = axes[1].pie(
                source_counts.values, 
                labels=source_counts.index,
                autopct='%1.1f%%',
                colors=colors,
                explode=[0.05] * len(source_counts)
            )
            axes[1].set_title('Distribution by Source Type', fontsize=13, fontweight='bold')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        
        return fig
    
    def plot_depth_analysis(self, save_path=None):
        """Plot depth analysis charts."""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        if 'depth_m' in self.df.columns:
            depth_data = self.df['depth_m'].dropna()
            
            # Histogram of depths
            axes[0, 0].hist(depth_data, bins=50, color='steelblue', edgecolor='white', alpha=0.7)
            axes[0, 0].axvline(depth_data.mean(), color='red', linestyle='--', 
                              label=f'Mean: {depth_data.mean():.1f}m')
            axes[0, 0].axvline(depth_data.median(), color='orange', linestyle='--', 
                              label=f'Median: {depth_data.median():.1f}m')
            axes[0, 0].set_xlabel('Depth (meters)', fontsize=11)
            axes[0, 0].set_ylabel('Frequency', fontsize=11)
            axes[0, 0].set_title('Distribution of Well Depths', fontsize=13, fontweight='bold')
            axes[0, 0].legend()
            
            # Box plot by source type
            if 'source_type' in self.df.columns:
                source_types = self.df['source_type'].unique()
                data_to_plot = [self.df[self.df['source_type'] == st]['depth_m'].dropna().values 
                               for st in source_types]
                bp = axes[0, 1].boxplot(data_to_plot, labels=source_types, patch_artist=True)
                
                colors = plt.cm.Set2(np.linspace(0, 1, len(source_types)))
                for patch, color in zip(bp['boxes'], colors):
                    patch.set_facecolor(color)
                
                axes[0, 1].set_ylabel('Depth (meters)', fontsize=11)
                axes[0, 1].set_title('Depth Distribution by Source Type', fontsize=13, fontweight='bold')
                axes[0, 1].tick_params(axis='x', rotation=45)
        
        # Depth category distribution
        if 'depth_category' in self.df.columns:
            depth_cat_counts = self.df['depth_category'].value_counts()
            colors = plt.cm.coolwarm(np.linspace(0.2, 0.8, len(depth_cat_counts)))
            axes[1, 0].bar(depth_cat_counts.index, depth_cat_counts.values, color=colors)
            axes[1, 0].set_xlabel('Depth Category', fontsize=11)
            axes[1, 0].set_ylabel('Count', fontsize=11)
            axes[1, 0].set_title('Wells by Depth Category', fontsize=13, fontweight='bold')
            axes[1, 0].tick_params(axis='x', rotation=45)
        
        # Average depth by county (top 10)
        if 'depth_m' in self.df.columns and 'county' in self.df.columns:
            avg_depth_county = (self.df.groupby('county')['depth_m']
                               .mean()
                               .sort_values(ascending=False)
                               .head(10))
            colors = plt.cm.Blues(np.linspace(0.3, 0.9, len(avg_depth_county)))
            axes[1, 1].barh(avg_depth_county.index, avg_depth_county.values, color=colors)
            axes[1, 1].set_xlabel('Average Depth (meters)', fontsize=11)
            axes[1, 1].set_title('Top 10 Counties by Average Well Depth', fontsize=13, fontweight='bold')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        
        return fig
    
    def plot_yield_analysis(self, save_path=None):
        """Plot yield analysis charts."""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        if 'yield_m3_day' in self.df.columns:
            yield_data = self.df['yield_m3_day'].dropna()
            yield_data_clipped = yield_data[yield_data <= yield_data.quantile(0.95)]
            
            # Histogram of yields
            axes[0, 0].hist(yield_data_clipped, bins=50, color='forestgreen', 
                          edgecolor='white', alpha=0.7)
            axes[0, 0].axvline(yield_data.mean(), color='red', linestyle='--', 
                              label=f'Mean: {yield_data.mean():.1f} m³/day')
            axes[0, 0].set_xlabel('Yield (m³/day)', fontsize=11)
            axes[0, 0].set_ylabel('Frequency', fontsize=11)
            axes[0, 0].set_title('Distribution of Well Yields (95th percentile)', 
                               fontsize=13, fontweight='bold')
            axes[0, 0].legend()
        
        # Yield category distribution
        if 'yield_category' in self.df.columns:
            yield_cat_counts = self.df['yield_category'].value_counts()
            colors = plt.cm.Greens(np.linspace(0.3, 0.9, len(yield_cat_counts)))
            
            wedges, texts, autotexts = axes[0, 1].pie(
                yield_cat_counts.values,
                labels=yield_cat_counts.index,
                autopct='%1.1f%%',
                colors=colors
            )
            axes[0, 1].set_title('Distribution by Yield Category', fontsize=13, fontweight='bold')
        
        # Yield by source type
        if 'yield_m3_day' in self.df.columns and 'source_type' in self.df.columns:
            avg_yield = self.df.groupby('source_type')['yield_m3_day'].mean().sort_values(ascending=True)
            colors = plt.cm.YlGn(np.linspace(0.3, 0.9, len(avg_yield)))
            axes[1, 0].barh(avg_yield.index, avg_yield.values, color=colors)
            axes[1, 0].set_xlabel('Average Yield (m³/day)', fontsize=11)
            axes[1, 0].set_title('Average Yield by Source Type', fontsize=13, fontweight='bold')
        
        # Top counties by yield
        if 'yield_m3_day' in self.df.columns and 'county' in self.df.columns:
            avg_yield_county = (self.df.groupby('county')['yield_m3_day']
                               .mean()
                               .sort_values(ascending=False)
                               .head(10))
            colors = plt.cm.Greens(np.linspace(0.3, 0.9, len(avg_yield_county)))
            axes[1, 1].barh(avg_yield_county.index, avg_yield_county.values, color=colors)
            axes[1, 1].set_xlabel('Average Yield (m³/day)', fontsize=11)
            axes[1, 1].set_title('Top 10 Counties by Average Yield', fontsize=13, fontweight='bold')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        
        return fig
    
    def plot_depth_yield_correlation(self, save_path=None):
        """Plot correlation between depth and yield."""
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        if 'depth_m' in self.df.columns and 'yield_m3_day' in self.df.columns:
            # Scatter plot
            valid_data = self.df.dropna(subset=['depth_m', 'yield_m3_day'])
            
            # Clip outliers for visualization
            valid_data = valid_data[
                (valid_data['yield_m3_day'] <= valid_data['yield_m3_day'].quantile(0.95)) &
                (valid_data['depth_m'] <= valid_data['depth_m'].quantile(0.95))
            ]
            
            axes[0].scatter(valid_data['depth_m'], valid_data['yield_m3_day'], 
                          alpha=0.5, c='steelblue', s=30)
            
            # Add trend line
            if len(valid_data) > 2:
                z = np.polyfit(valid_data['depth_m'], valid_data['yield_m3_day'], 1)
                p = np.poly1d(z)
                x_line = np.linspace(valid_data['depth_m'].min(), valid_data['depth_m'].max(), 100)
                axes[0].plot(x_line, p(x_line), 'r--', linewidth=2, label='Trend line')
            
            axes[0].set_xlabel('Depth (meters)', fontsize=11)
            axes[0].set_ylabel('Yield (m³/day)', fontsize=11)
            axes[0].set_title('Depth vs Yield Correlation', fontsize=13, fontweight='bold')
            
            # Calculate correlation
            corr = valid_data['depth_m'].corr(valid_data['yield_m3_day'])
            axes[0].text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=axes[0].transAxes,
                        fontsize=11, verticalalignment='top', 
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
            axes[0].legend()
        
        # Heatmap of average yield by depth and source type
        if all(col in self.df.columns for col in ['depth_category', 'source_type', 'yield_m3_day']):
            pivot_data = self.df.pivot_table(
                values='yield_m3_day', 
                index='depth_category', 
                columns='source_type',
                aggfunc='mean'
            )
            
            if not pivot_data.empty:
                sns.heatmap(pivot_data, annot=True, fmt='.1f', cmap='YlGnBu', ax=axes[1])
                axes[1].set_title('Average Yield by Depth Category & Source Type', 
                                fontsize=13, fontweight='bold')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        
        return fig
    
    def plot_data_quality(self, save_path=None):
        """Plot data quality metrics."""
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        if 'completeness_score' in self.df.columns:
            # Histogram of completeness scores
            axes[0].hist(self.df['completeness_score'].dropna(), bins=20, 
                        color='purple', edgecolor='white', alpha=0.7)
            axes[0].axvline(self.df['completeness_score'].mean(), color='red', linestyle='--',
                          label=f'Mean: {self.df["completeness_score"].mean():.1f}')
            axes[0].set_xlabel('Completeness Score', fontsize=11)
            axes[0].set_ylabel('Frequency', fontsize=11)
            axes[0].set_title('Distribution of Data Completeness Scores', 
                            fontsize=13, fontweight='bold')
            axes[0].legend()
        
        # Data availability by field
        field_availability = {}
        important_fields = ['well_id', 'county', 'source_type', 'depth_m', 
                          'yield_m3_day', 'latitude', 'longitude']
        
        for field in important_fields:
            if field in self.df.columns:
                non_null = self.df[field].notna().sum()
                field_availability[field] = (non_null / len(self.df)) * 100
        
        if field_availability:
            fields = list(field_availability.keys())
            values = list(field_availability.values())
            colors = ['green' if v > 80 else 'orange' if v > 50 else 'red' for v in values]
            
            axes[1].barh(fields, values, color=colors)
            axes[1].set_xlabel('Data Availability (%)', fontsize=11)
            axes[1].set_title('Data Completeness by Field', fontsize=13, fontweight='bold')
            axes[1].axvline(x=80, color='green', linestyle='--', alpha=0.5)
            axes[1].axvline(x=50, color='orange', linestyle='--', alpha=0.5)
            axes[1].set_xlim(0, 100)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        
        return fig
    
    def get_insights(self):
        """Generate key insights from the data."""
        insights = []
        
        summary = self.get_summary_statistics()
        insights.append(f"Dataset contains {summary['total_records']:,} groundwater source records")
        
        if summary['unique_counties']:
            insights.append(f"Data covers {summary['unique_counties']} Irish counties")
        
        if summary.get('source_types'):
            dominant = max(summary['source_types'], key=summary['source_types'].get)
            insights.append(f"Most common source type: {dominant} ({summary['source_types'][dominant]:,} records)")
        
        if summary['avg_depth_m']:
            insights.append(f"Average well depth: {summary['avg_depth_m']:.1f} meters")
        
        if summary['avg_yield_m3_day']:
            insights.append(f"Average yield: {summary['avg_yield_m3_day']:.1f} m³/day")
        
        if 'depth_m' in self.df.columns and 'yield_m3_day' in self.df.columns:
            valid = self.df.dropna(subset=['depth_m', 'yield_m3_day'])
            if len(valid) > 10:
                corr = valid['depth_m'].corr(valid['yield_m3_day'])
                insights.append(f"Depth-Yield correlation: {corr:.3f} ({'positive' if corr > 0 else 'negative'})")
        
        return insights


def run_ireland_analysis(df, output_dir='charts'):
    """Run complete Ireland Groundwater analysis and save charts."""
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    analyzer = IrelandGroundwaterAnalyzer(df)
    
    logger.info("Ireland Groundwater Data Analysis")
    logger.info("=" * 50)
    
    # Summary
    summary = analyzer.get_summary_statistics()
    logger.info("Summary Statistics:")
    for key, value in summary.items():
        if not isinstance(value, dict):
            logger.info(f"  {key}: {value}")
    
    # Generate charts
    logger.info("Generating charts...")
    
    analyzer.plot_county_distribution(
        save_path=f'{output_dir}/ireland_county_distribution.png')
    logger.info("  ✓ County distribution chart")
    
    analyzer.plot_depth_analysis(
        save_path=f'{output_dir}/ireland_depth_analysis.png')
    logger.info("  ✓ Depth analysis chart")
    
    analyzer.plot_yield_analysis(
        save_path=f'{output_dir}/ireland_yield_analysis.png')
    logger.info("  ✓ Yield analysis chart")
    
    analyzer.plot_depth_yield_correlation(
        save_path=f'{output_dir}/ireland_correlation.png')
    logger.info("  ✓ Correlation chart")
    
    analyzer.plot_data_quality(
        save_path=f'{output_dir}/ireland_data_quality.png')
    logger.info("  ✓ Data quality chart")
    
    # Insights
    logger.info("Key Insights:")
    for insight in analyzer.get_insights():
        logger.info(f"  • {insight}")
    
    return analyzer


if __name__ == "__main__":
    
    from db_utils import load_ireland_data
    
    logger.info("Loading Ireland Groundwater data from Mart...")
    df = load_ireland_data()
    logger.info(f"Loaded {len(df):,} records")
    
    analyzer = run_ireland_analysis(df, output_dir='charts/ireland')
